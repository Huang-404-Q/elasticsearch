/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.CannedSourceOperator;
import org.elasticsearch.compute.test.OperatorTestCase;
import org.elasticsearch.compute.test.TestDriverRunner;
import org.elasticsearch.core.Releasables;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class KeepMinimumOperatorTests extends OperatorTestCase {
    private record Row(String key, int priority, int value) {}

    @Override
    protected Operator.OperatorFactory simple(SimpleOptions options) {
        return new KeepMinimumOperator.Factory(0, 1);
    }

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        List<Page> pages = new ArrayList<>();
        try {
            int pageSize = randomPageSize();
            for (int start = 0; start < size; start += pageSize) {
                List<Row> rows = new ArrayList<>();
                for (int i = start; i < Math.min(size, start + pageSize); i++) {
                    rows.add(new Row(Integer.toString(i % 11), i % 7 - 3, i));
                }
                pages.add(page(blockFactory, rows));
            }
            return new CannedSourceOperator(pages.iterator());
        } catch (Exception e) {
            Releasables.close(pages);
            throw e;
        }
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        return equalTo("KeepMinimumOperator[keyChannel=0, priorityChannel=1]");
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return expectedDescriptionOfSimple();
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, List<Page> results) {
        assertEquals(expected(rows(input)), rows(results));
    }

    public void testInputOrderDoesNotChangeSelectedRows() {
        List<Row> input = new ArrayList<>();
        for (int i = 0; i < 250; i++) {
            input.add(new Row(Integer.toString(randomInt(12)), randomIntBetween(-3, 3), i));
        }
        Collections.shuffle(input, random());
        List<Page> pages = new ArrayList<>();
        for (Row row : input) {
            pages.add(page(driverContext().blockFactory(), List.of(row)));
        }
        List<Page> result = new TestDriverRunner().builder(driverContext()).input(pages.toArray(Page[]::new)).run(simple());
        try {
            assertEquals(expected(input), rows(result));
        } finally {
            Releasables.close(result);
        }
    }

    public void testLaterLowerPriorityPreservesAllTies() {
        List<Page> pages = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            pages.add(page(new Row("key", 1, i)));
        }
        pages.add(page(new Row("key", 0, 100), new Row("key", 0, 101)));
        List<Page> result = new TestDriverRunner().builder(driverContext()).input(pages.toArray(Page[]::new)).run(simple());
        try {
            assertEquals(List.of(new Row("key", 0, 100), new Row("key", 0, 101)), rows(result));
        } finally {
            Releasables.close(result);
        }
    }

    public void testLimitWaitsForLatePreferredRowAndReleasesRemainder() {
        Page earlier = page(new Row("key", 1, 1));
        Page preferred = page(new Row("key", 0, 2));
        Page tie = page(new Row("key", 0, 3));
        Block earlierBlock = earlier.getBlock(0);
        Block preferredBlock = preferred.getBlock(0);
        Block tieBlock = tie.getBlock(0);
        List<Page> result = new TestDriverRunner().builder(driverContext())
            .input(earlier, preferred, tie)
            .run(simple(), new LimitOperator.Factory(1));
        try {
            assertEquals(List.of(new Row("key", 0, 2)), rows(result));
        } finally {
            Releasables.close(result);
        }
        assertTrue(earlierBlock.isReleased());
        assertTrue(preferredBlock.isReleased());
        assertTrue(tieBlock.isReleased());
    }

    public void testWithholdsOutputAndReleasesOnEarlyClose() {
        Page first = page(new Row("key", 1, 1));
        Page second = page(new Row("key", 0, 2));
        Block firstBlock = first.getBlock(0);
        Block secondBlock = second.getBlock(0);
        try (Operator operator = simple().get(driverContext())) {
            operator.addInput(first);
            assertNull(operator.getOutput());
            operator.addInput(second);
            assertNull(operator.getOutput());
            assertEquals(0, operator.status().rowsEmitted());
            if (randomBoolean()) {
                operator.finish();
            }
        }
        assertTrue(firstBlock.isReleased());
        assertTrue(secondBlock.isReleased());
    }

    public void testEmptyPageAndInput() {
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

    public void testNullKeyFailureReleasesInputAndBufferedPages() {
        Page buffered = page(new Row("key", 0, 1));
        Page invalid;
        BlockFactory factory = driverContext().blockFactory();
        try (BytesRefBlock.Builder key = factory.newBytesRefBlockBuilder(1)) {
            key.appendNull();
            invalid = new Page(key.build(), factory.newConstantIntBlockWith(0, 1), factory.newConstantIntBlockWith(2, 1));
        }
        Block bufferedBlock = buffered.getBlock(0);
        Block invalidBlock = invalid.getBlock(0);
        expectThrows(
            IllegalArgumentException.class,
            () -> new TestDriverRunner().builder(driverContext()).input(buffered, invalid).run(simple())
        );
        assertTrue(bufferedBlock.isReleased());
        assertTrue(invalidBlock.isReleased());
    }

    public void testDistinctKeysWithTheSameLuceneHashStayDistinct() {
        Map<Integer, String> hashes = new HashMap<>();
        String first = null;
        String second = null;
        for (int i = 0; i < 2_000_000; i++) {
            String value = "collision-key-" + i;
            String previous = hashes.putIfAbsent(new BytesRef(value).hashCode(), value);
            if (previous != null) {
                first = previous;
                second = value;
                break;
            }
        }
        assertNotNull("Expected a collision in the 32-bit Lucene key hash", first);
        assertNotEquals(first, second);
        List<Row> input = List.of(new Row(first, 0, 1), new Row(second, 1, 2), new Row(second, 1, 3));
        List<Page> result = new TestDriverRunner().builder(driverContext())
            .input(page(driverContext().blockFactory(), input))
            .run(simple());
        try {
            assertEquals(input, rows(result));
        } finally {
            Releasables.close(result);
        }
    }

    public void testNullAndMultivaluedPrioritiesAreRejected() {
        BlockFactory factory = driverContext().blockFactory();
        for (boolean multiple : List.of(false, true)) {
            Page invalid;
            try (IntBlock.Builder priority = factory.newIntBlockBuilder(1)) {
                if (multiple) {
                    priority.beginPositionEntry().appendInt(0).appendInt(1).endPositionEntry();
                } else {
                    priority.appendNull();
                }
                invalid = new Page(
                    factory.newConstantBytesRefBlockWith(new BytesRef("key"), 1),
                    priority.build(),
                    factory.newConstantIntBlockWith(1, 1)
                );
            }
            Block block = invalid.getBlock(1);
            expectThrows(
                IllegalArgumentException.class,
                () -> new TestDriverRunner().builder(driverContext()).input(invalid).run(simple())
            );
            assertTrue(block.isReleased());
        }
    }

    public void testMultivaluedKeysAreRejected() {
        BlockFactory factory = driverContext().blockFactory();
        Page invalid;
        try (BytesRefBlock.Builder key = factory.newBytesRefBlockBuilder(1)) {
            key.beginPositionEntry().appendBytesRef(new BytesRef("a")).appendBytesRef(new BytesRef("b")).endPositionEntry();
            invalid = new Page(key.build(), factory.newConstantIntBlockWith(0, 1), factory.newConstantIntBlockWith(1, 1));
        }
        Block block = invalid.getBlock(0);
        expectThrows(IllegalArgumentException.class, () -> new TestDriverRunner().builder(driverContext()).input(invalid).run(simple()));
        assertTrue(block.isReleased());
    }

    private Page page(Row... rows) {
        return page(driverContext().blockFactory(), List.of(rows));
    }

    private static Page page(BlockFactory factory, List<Row> rows) {
        try (
            BytesRefBlock.Builder keys = factory.newBytesRefBlockBuilder(rows.size());
            IntBlock.Builder priorities = factory.newIntBlockBuilder(rows.size());
            IntBlock.Builder values = factory.newIntBlockBuilder(rows.size())
        ) {
            for (Row row : rows) {
                keys.appendBytesRef(new BytesRef(row.key()));
                priorities.appendInt(row.priority());
                values.appendInt(row.value());
            }
            Block[] blocks = new Block[3];
            try {
                blocks[0] = keys.build();
                blocks[1] = priorities.build();
                blocks[2] = values.build();
                return new Page(blocks);
            } catch (Exception e) {
                Releasables.close(blocks);
                throw e;
            }
        }
    }

    private static List<Row> expected(List<Row> input) {
        Map<String, Integer> minima = new HashMap<>();
        input.forEach(row -> minima.merge(row.key(), row.priority(), Math::min));
        return input.stream().filter(row -> row.priority() == minima.get(row.key())).toList();
    }

    private static List<Row> rows(List<Page> pages) {
        List<Row> rows = new ArrayList<>();
        BytesRef scratch = new BytesRef();
        for (Page page : pages) {
            BytesRefBlock keys = page.getBlock(0);
            IntBlock priorities = page.getBlock(1);
            IntBlock values = page.getBlock(2);
            for (int p = 0; p < page.getPositionCount(); p++) {
                rows.add(
                    new Row(
                        keys.getBytesRef(keys.getFirstValueIndex(p), scratch).utf8ToString(),
                        priorities.getInt(priorities.getFirstValueIndex(p)),
                        values.getInt(values.getFirstValueIndex(p))
                    )
                );
            }
        }
        return rows;
    }
}
