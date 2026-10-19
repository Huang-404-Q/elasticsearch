/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.OperatorTestCase;
import org.elasticsearch.compute.test.TestDriverRunner;
import org.elasticsearch.compute.test.operator.blocksource.SequenceBytesRefBlockSourceOperator;
import org.elasticsearch.core.Releasables;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;

public class FullInputDistinctByOperatorTests extends OperatorTestCase {
    @Override
    protected Operator.OperatorFactory simple(SimpleOptions options) {
        return new DistinctByOperator.FullInputBytesRefKeyFactory(0);
    }

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        return new SequenceBytesRefBlockSourceOperator(
            blockFactory,
            IntStream.range(0, size).mapToObj(i -> new BytesRef(Integer.toString(i))),
            randomPageSize()
        );
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        return equalTo("UniquenessGuard[keyChannel=0]");
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return startsWith("UniquenessGuard[DistinctByOperator[keyChannel=0, processor=");
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, List<Page> results) {
        assertEquals(keys(input), keys(results));
    }

    public void testWithholdsPagesUntilCompleteInput() {
        try (Operator operator = simple().get(driverContext())) {
            Page first = page("a");
            Page second = page("b");
            operator.addInput(first);
            assertNull(operator.getOutput());
            assertTrue(operator.needsInput());
            operator.addInput(second);
            assertNull(operator.getOutput());
            assertEquals(0, operator.status().rowsEmitted());
            assertFalse(operator.isFinished());
            operator.finish();
            Page firstOutput = operator.getOutput();
            assertSame(first, firstOutput);
            firstOutput.releaseBlocks();
            Page secondOutput = operator.getOutput();
            assertSame(second, secondOutput);
            secondOutput.releaseBlocks();
            assertTrue(operator.isFinished());
        }
    }

    public void testLimitDoesNotHideLaterDuplicate() {
        Page first = page("a");
        Page duplicate = page("a");
        BytesRefBlock firstBlock = first.getBlock(0);
        BytesRefBlock duplicateBlock = duplicate.getBlock(0);
        var runner = new TestDriverRunner().builder(driverContext()).input(first, duplicate);
        IllegalArgumentException failure = expectThrows(
            IllegalArgumentException.class,
            () -> runner.run(new DistinctByOperator.FullInputBytesRefKeyFactory(0), new LimitOperator.Factory(1))
        );
        assertEquals("input must not have duplicates when [failOnDuplicate] set to [true]", failure.getMessage());
        assertTrue(firstBlock.isReleased());
        assertTrue(duplicateBlock.isReleased());
    }

    public void testCloseReleasesRetainedAndPendingPages() {
        Page retained = page("a");
        Page pending = page("b");
        BytesRefBlock retainedBlock = retained.getBlock(0);
        BytesRefBlock pendingBlock = pending.getBlock(0);
        try (Operator operator = simple().get(driverContext())) {
            operator.addInput(retained);
            assertNull(operator.getOutput());
            operator.addInput(pending);
            if (randomBoolean()) {
                operator.finish();
            }
        }
        assertTrue(retainedBlock.isReleased());
        assertTrue(pendingBlock.isReleased());
    }

    public void testLimitReleasesUnconsumedCheckedPages() {
        Page first = page("a");
        Page second = page("b");
        BytesRefBlock firstBlock = first.getBlock(0);
        BytesRefBlock secondBlock = second.getBlock(0);
        List<Page> results = new TestDriverRunner().builder(driverContext())
            .input(first, second)
            .run(new DistinctByOperator.FullInputBytesRefKeyFactory(0), new LimitOperator.Factory(1));
        try {
            assertEquals(List.of("a"), keys(results));
        } finally {
            Releasables.close(results);
        }
        assertTrue(firstBlock.isReleased());
        assertTrue(secondBlock.isReleased());
    }

    public void testEmptyPageAndEmptyInput() {
        try (Operator operator = simple().get(driverContext())) {
            if (randomBoolean()) {
                operator.addInput(page());
                assertNull(operator.getOutput());
            }
            operator.finish();
            assertTrue(operator.isFinished());
            assertNull(operator.getOutput());
        }
    }

    private Page page(String... keys) {
        try (BytesRefBlock.Builder builder = driverContext().blockFactory().newBytesRefBlockBuilder(keys.length)) {
            for (String key : keys) {
                builder.appendBytesRef(new BytesRef(key));
            }
            return new Page(builder.build());
        }
    }

    private static List<String> keys(List<Page> pages) {
        List<String> keys = new ArrayList<>();
        BytesRef scratch = new BytesRef();
        for (Page page : pages) {
            BytesRefBlock block = page.getBlock(0);
            for (int position = 0; position < page.getPositionCount(); position++) {
                keys.add(block.getBytesRef(block.getFirstValueIndex(position), scratch).utf8ToString());
            }
        }
        return keys;
    }
}
