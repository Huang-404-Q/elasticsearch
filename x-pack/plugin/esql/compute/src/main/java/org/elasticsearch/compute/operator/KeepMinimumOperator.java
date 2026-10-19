/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BytesRefHashTable;
import org.elasticsearch.common.util.IntArray;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.compute.aggregation.blockhash.HashImplFactory;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;

/**
 * Retains every row whose integer priority is minimal for its exact byte key, including all ties.
 * Input order does not affect membership. Output waits for the complete input because a later row
 * may establish a lower priority. Keys and priorities must be non-null and single-valued.
 */
public final class KeepMinimumOperator implements Operator {
    /** Builds an operator over an exact byte key and an integer priority. */
    public record Factory(int keyChannel, int priorityChannel) implements OperatorFactory {
        @Override
        public Operator get(DriverContext context) {
            return new KeepMinimumOperator(keyChannel, priorityChannel, context);
        }

        @Override
        public String describe() {
            return "KeepMinimumOperator[keyChannel=" + keyChannel + ", priorityChannel=" + priorityChannel + "]";
        }
    }

    private final int keyChannel;
    private final int priorityChannel;
    private final BigArrays bigArrays;
    private final BlockFactory blockFactory;
    private final BytesRefHashTable keys;
    private final BytesRef scratch = new BytesRef();
    private IntArray minima;
    private ObjectArray<Page> pages;
    private long readPosition;
    private long writePosition;
    private boolean finished;
    private boolean closed;
    private long processNanos;
    private int pagesProcessed;
    private long rowsReceived;
    private long rowsEmitted;

    private KeepMinimumOperator(int keyChannel, int priorityChannel, DriverContext context) {
        this.keyChannel = keyChannel;
        this.priorityChannel = priorityChannel;
        this.bigArrays = context.bigArrays().withCircuitBreaking();
        this.blockFactory = context.blockFactory();
        this.keys = HashImplFactory.newBytesRefHash(blockFactory);
    }

    @Override
    public boolean needsInput() {
        return finished == false && closed == false;
    }

    @Override
    public void addInput(Page page) {
        long start = System.nanoTime();
        boolean retained = false;
        try {
            if (needsInput() == false) {
                throw new IllegalStateException("Cannot accept input after completion");
            }
            pagesProcessed++;
            rowsReceived += page.getPositionCount();
            if (page.getPositionCount() == 0) {
                return;
            }
            BytesRefVector keyVector = ((BytesRefBlock) page.getBlock(keyChannel)).asVector();
            IntVector priorityVector = ((IntBlock) page.getBlock(priorityChannel)).asVector();
            if (keyVector == null || priorityVector == null) {
                throw new IllegalArgumentException("Keys and priorities must be non-null and single-valued");
            }
            for (int p = 0; p < page.getPositionCount(); p++) {
                long ordinal = keys.add(keyVector.getBytesRef(p, scratch));
                int priority = priorityVector.getInt(p);
                if (ordinal >= 0) {
                    minima = minima == null ? bigArrays.newIntArray(ordinal + 1, false) : bigArrays.grow(minima, ordinal + 1);
                    minima.set(ordinal, priority);
                } else {
                    ordinal = -1 - ordinal;
                    minima.set(ordinal, Math.min(minima.get(ordinal), priority));
                }
            }
            pages = pages == null ? bigArrays.newObjectArray(1) : bigArrays.grow(pages, writePosition + 1);
            pages.set(writePosition++, page);
            retained = true;
        } finally {
            processNanos += System.nanoTime() - start;
            if (retained == false) {
                page.releaseBlocks();
            }
        }
    }

    @Override
    public void finish() {
        finished = true;
    }

    @Override
    public boolean isFinished() {
        return closed || finished && readPosition == writePosition;
    }

    @Override
    public boolean canProduceMoreDataWithoutExtraInput() {
        return closed == false && finished && readPosition < writePosition;
    }

    @Override
    public Page getOutput() {
        if (canProduceMoreDataWithoutExtraInput() == false) {
            return null;
        }
        long start = System.nanoTime();
        Page page = pages.get(readPosition);
        pages.set(readPosition++, null);
        boolean returned = false;
        long selectionBytes = RamUsageEstimator.alignObjectSize(
            RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) Integer.BYTES * page.getPositionCount()
        );
        boolean reserved = false;
        try {
            BytesRefVector keyVector = ((BytesRefBlock) page.getBlock(keyChannel)).asVector();
            IntVector priorityVector = ((IntBlock) page.getBlock(priorityChannel)).asVector();
            blockFactory.adjustBreaker(selectionBytes);
            reserved = true;
            int[] selected = new int[page.getPositionCount()];
            int count = 0;
            for (int p = 0; p < page.getPositionCount(); p++) {
                long ordinal = keys.find(keyVector.getBytesRef(p, scratch));
                if (priorityVector.getInt(p) == minima.get(ordinal)) {
                    selected[count++] = p;
                }
            }
            rowsEmitted += count;
            if (count == page.getPositionCount()) {
                returned = true;
                return page;
            }
            return count == 0 ? null : page.filter(false, selected, 0, count);
        } finally {
            processNanos += System.nanoTime() - start;
            if (reserved) {
                blockFactory.adjustBreaker(-selectionBytes);
            }
            if (returned == false) {
                page.releaseBlocks();
            }
        }
    }

    @Override
    public AbstractPageMappingOperator.Status status() {
        return new AbstractPageMappingOperator.Status(processNanos, pagesProcessed, rowsReceived, rowsEmitted);
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        try {
            while (readPosition < writePosition) {
                Page page = pages.get(readPosition);
                pages.set(readPosition++, null);
                page.releaseBlocks();
            }
        } finally {
            Releasables.close(pages, minima, keys);
        }
    }

    @Override
    public String toString() {
        return new Factory(keyChannel, priorityChannel).describe();
    }
}
