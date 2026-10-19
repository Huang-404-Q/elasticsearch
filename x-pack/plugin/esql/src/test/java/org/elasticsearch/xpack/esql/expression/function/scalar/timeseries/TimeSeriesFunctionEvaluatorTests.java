/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.timeseries;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.OrdinalBytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.Case;
import org.elasticsearch.xpack.esql.expression.function.scalar.nulls.Coalesce;
import org.junit.After;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.compute.data.BlockUtils.fromArrayRow;
import static org.elasticsearch.xpack.esql.core.util.NumericUtils.asLongUnsigned;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.stringToIP;
import static org.hamcrest.Matchers.containsString;

/** Executes the public functions through real evaluators, including row-dependent dictionary overlays. */
public class TimeSeriesFunctionEvaluatorTests extends ESTestCase {
    private static final FieldAttribute METADATA = field("metadata", DataType.TIMESERIES_DATA_TYPE);
    private final List<CircuitBreaker> breakers = new ArrayList<>();

    /** Null metadata is empty rather than null-propagating, and missing labels remain nullable. */
    public void testEmptyMetadata() {
        for (Object empty : new Object[] { null, new BytesRef("{}") }) {
            assertNull(evaluate(new TimeSeriesGet(Source.EMPTY, METADATA, label("pod")), empty));
            assertEquals("{}", evaluate(new TimeSeriesUnset(Source.EMPTY, METADATA, List.of(label("pod"))), empty));
            assertEquals("{}", evaluate(new TimeSeriesHash(Source.EMPTY, METADATA, List.of()), empty));
            assertEquals("{}", evaluate(new TimeSeriesToString(Source.EMPTY, METADATA, List.of()), empty));
            assertEquals("{\"pod\":\"a\"}", evaluate(new TimeSeriesSet(Source.EMPTY, METADATA, label("pod"), label("a")), empty));
        }
    }

    /** Removing a label is idempotent, including repeated names and absent or empty replacements. */
    public void testRemovalsAndNoOpTransforms() {
        BytesRef metadata = new BytesRef("{\"pod\":\"a\",\"job\":\"api\"}");
        for (Expression value : List.of(Literal.NULL, label(""))) {
            assertEquals("{\"job\":\"api\"}", evaluate(new TimeSeriesSet(Source.EMPTY, METADATA, label("pod"), value), metadata));
        }
        assertEquals(
            "{\"job\":\"api\"}",
            evaluate(new TimeSeriesUnset(Source.EMPTY, METADATA, List.of(label("pod"), label("pod"), label("missing"))), metadata)
        );
        assertEquals(
            "{\"job\":\"api\",\"pod\":\"a\"}",
            evaluate(new TimeSeriesSet(Source.EMPTY, METADATA, label("pod"), label("a")), metadata)
        );
        assertEquals(
            "{\"job\":\"api\",\"pod\":\"a\"}",
            evaluate(new TimeSeriesUnset(Source.EMPTY, METADATA, List.of(label("missing"))), metadata)
        );
        assertNull(evaluate(new TimeSeriesGet(Source.EMPTY, METADATA, label("pod")), new BytesRef("{\"pod\":\"\"}")));
    }

    /** Promoted columns and encoded labels must produce the same full matching key. */
    public void testCanonicalKeysAndOverlays() {
        var hash = new TimeSeriesHash(Source.EMPTY, METADATA, List.of());
        String key = evaluate(hash, new BytesRef("{ \"pod\": \"a\", \"job\": \"api\" }"));
        assertEquals(key, evaluate(hash, new BytesRef("{\"job\":\"api\",\"pod\":\"a\"}")));
        for (TimeSeriesFunction overlay : List.of(
            new TimeSeriesHash(Source.EMPTY, METADATA, List.of(label("pod"), label("a"))),
            new TimeSeriesToString(Source.EMPTY, METADATA, List.of(label("pod"), label("a")))
        )) {
            assertEquals(key, evaluate(overlay, new BytesRef("{\"job\":\"api\",\"pod\":\"old\"}")));
        }
        assertEquals(
            "{\"job\":\"api\"}",
            evaluate(
                new TimeSeriesHash(Source.EMPTY, METADATA, List.of(label("pod"), Literal.NULL)),
                new BytesRef("{\"pod\":\"a\",\"job\":\"api\"}")
            )
        );
        assertNotEquals(key, evaluate(hash, new BytesRef("{\"job\":\"api\",\"pod\":\"b\"}")));
    }

    /** Values retain their label representation, including unsigned values and encoded IP addresses. */
    public void testTypedValues() {
        assertValue(DataType.KEYWORD, new BytesRef("a"), "a");
        assertValue(DataType.TEXT, new BytesRef("a"), "a");
        assertValue(DataType.BOOLEAN, false, "false");
        assertValue(DataType.INTEGER, 0, "0");
        assertValue(DataType.LONG, Long.MIN_VALUE, Long.toString(Long.MIN_VALUE));
        assertValue(DataType.UNSIGNED_LONG, asLongUnsigned(BigInteger.ZERO), "0");
        assertValue(DataType.UNSIGNED_LONG, asLongUnsigned(BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE)), "18446744073709551615");
        assertValue(DataType.DOUBLE, -0.0, "-0.0");
        assertValue(DataType.DOUBLE, 1.25, "1.25");
        assertValue(DataType.IP, stringToIP("192.0.2.1"), "192.0.2.1");
        assertValue(DataType.IP, stringToIP("2001:db8::1"), "2001:db8::1");
    }

    /** Escaping and non-ASCII data must survive SET/GET without changing label identity. */
    public void testEscapedValues() {
        String name = "label\"\\\n";
        String value = "value\"\\\nλ😀";
        var set = new TimeSeriesSet(Source.EMPTY, METADATA, label(name), label(value));
        String encoded = evaluate(set, (Object) null);
        assertEquals(value, evaluate(new TimeSeriesGet(Source.EMPTY, METADATA, label(name)), new BytesRef(encoded)));
        assertEquals(encoded, evaluate(new TimeSeriesHash(Source.EMPTY, METADATA, List.of()), new BytesRef(encoded)));
    }

    /** Shared metadata ordinals do not imply shared values for promoted columns. */
    public void testRowDependentOverlaysOnOrdinalMetadata() {
        var value = field("value", DataType.KEYWORD);
        for (TimeSeriesFunction expression : List.of(
            new TimeSeriesSet(Source.EMPTY, METADATA, label("pod"), value),
            new TimeSeriesHash(Source.EMPTY, METADATA, List.of(label("pod"), value)),
            new TimeSeriesToString(Source.EMPTY, METADATA, List.of(label("pod"), value))
        )) {
            DriverContext context = driverContext();
            try (
                BytesRefVector.Builder dictionary = context.blockFactory().newBytesRefVectorBuilder(1);
                IntBlock.Builder ordinals = context.blockFactory().newIntBlockBuilder(3);
                BytesRefBlock.Builder values = context.blockFactory().newBytesRefBlockBuilder(3)
            ) {
                dictionary.appendBytesRef(new BytesRef("{\"job\":\"api\"}"));
                ordinals.appendInt(0).appendInt(0).appendInt(0);
                values.appendBytesRef(new BytesRef("a")).appendBytesRef(new BytesRef("b")).appendNull();
                Page page = new Page(new OrdinalBytesRefBlock(ordinals.build(), dictionary.build()), values.build());
                try (ExpressionEvaluator evaluator = AbstractScalarFunctionTestCase.evaluator(expression).get(context)) {
                    try (BytesRefBlock result = (BytesRefBlock) evaluator.eval(page)) {
                        assertEquals("{\"job\":\"api\",\"pod\":\"a\"}", result.getBytesRef(0, new BytesRef()).utf8ToString());
                        assertEquals("{\"job\":\"api\",\"pod\":\"b\"}", result.getBytesRef(1, new BytesRef()).utf8ToString());
                        assertEquals("{\"job\":\"api\"}", result.getBytesRef(2, new BytesRef()).utf8ToString());
                    }
                } finally {
                    page.releaseBlocks();
                }
            }
        }
    }

    /** Invalid metadata must fail rather than silently merge series through a null result. */
    public void testMalformedMetadataFails() {
        for (String invalid : List.of("not JSON", "[]", "{\"pod\":3}", "{\"pod\":\"a\",\"pod\":\"b\"}", "{} {}")) {
            expectThrows(
                RuntimeException.class,
                () -> evaluate(new TimeSeriesHash(Source.EMPTY, METADATA, List.of()), new BytesRef(invalid))
            );
        }
    }

    /** Multivalued inputs cannot be arbitrarily selected or converted to a missing label. */
    public void testMultivaluedInputsFail() {
        var get = new TimeSeriesGet(Source.EMPTY, METADATA, label("pod"));
        var error = expectThrows(
            IllegalArgumentException.class,
            () -> evaluate(get, List.of(new BytesRef("{\"pod\":\"a\"}"), new BytesRef("{\"pod\":\"b\"}")))
        );
        assertThat(error.getMessage(), containsString("must be single-valued"));
        var value = field("value", DataType.KEYWORD);
        for (TimeSeriesFunction expression : List.of(
            new TimeSeriesSet(Source.EMPTY, METADATA, label("pod"), value),
            new TimeSeriesHash(Source.EMPTY, METADATA, List.of(label("pod"), value)),
            new TimeSeriesToString(Source.EMPTY, METADATA, List.of(label("pod"), value))
        )) {
            error = expectThrows(
                IllegalArgumentException.class,
                () -> evaluate(expression, null, List.of(new BytesRef("a"), new BytesRef("b")))
            );
            assertThat(error.getMessage(), containsString("must be single-valued"));
        }
    }

    /** Breaker-accounted metadata is not subject to the arbitrary limit for unaccounted scalar strings. */
    public void testLargeResult() {
        var set = new TimeSeriesSet(Source.EMPTY, METADATA, label("pod"), field("value", DataType.KEYWORD));
        String value = "x".repeat((1 << 20) + 128);
        assertEquals("{\"pod\":\"" + value + "\"}", evaluate(set, null, new BytesRef(value)));
    }

    /** A breaker trip releases evaluated inputs, row scratch, and partially constructed output. */
    public void testCircuitBreakerFailureReleasesMemory() {
        DriverContext context = driverContext(ByteSizeValue.ofKb(32));
        var set = new TimeSeriesSet(Source.EMPTY, METADATA, label("pod"), field("value", DataType.KEYWORD));
        Page page = new Page(fromArrayRow(context.blockFactory(), null, new BytesRef("x".repeat(1024))));
        try (ExpressionEvaluator evaluator = AbstractScalarFunctionTestCase.evaluator(set).get(context)) {
            expectThrows(CircuitBreakingException.class, () -> {
                try (var unexpected = evaluator.eval(page)) {
                    // Close an unexpected successful result so a failed assertion does not leak it.
                }
            });
        } finally {
            page.releaseBlocks();
        }
    }

    /** Constant expressions use the same execution semantics as column-backed expressions. */
    public void testFolding() {
        var set = new TimeSeriesSet(Source.EMPTY, Literal.NULL, label("pod"), label("a"));
        assertTrue(set.foldable());
        assertEquals(new BytesRef("{\"pod\":\"a\"}"), set.fold(FoldContext.small()));
        assertEquals(new BytesRef("a"), new TimeSeriesGet(Source.EMPTY, set, label("pod")).fold(FoldContext.small()));
        assertEquals(new BytesRef("{}"), new TimeSeriesUnset(Source.EMPTY, set, List.of(label("pod"))).fold(FoldContext.small()));
        assertEquals(new BytesRef("{\"pod\":\"a\"}"), new TimeSeriesHash(Source.EMPTY, set, List.of()).fold(FoldContext.small()));
    }

    /** Generic conditional expressions retain opaque metadata until explicit rendering. */
    public void testTypedConditionalExpressions() {
        var fallback = new TimeSeriesSet(Source.EMPTY, Literal.NULL, label("pod"), label("fallback"));
        var coalesce = new Coalesce(Source.EMPTY, METADATA, List.of(fallback));
        var rendered = new TimeSeriesToString(Source.EMPTY, coalesce, List.of());
        assertEquals("{\"pod\":\"fallback\"}", evaluate(rendered, (Object) null));
        assertEquals("{\"pod\":\"original\"}", evaluate(rendered, new BytesRef("{\"pod\":\"original\"}")));

        var set = new TimeSeriesSet(Source.EMPTY, METADATA, label("pod"), label("changed"));
        rendered = new TimeSeriesToString(Source.EMPTY, new Coalesce(Source.EMPTY, set, List.of(fallback)), List.of());
        assertEquals("{\"pod\":\"changed\"}", evaluate(rendered, (Object) null));

        var conditional = new Case(Source.EMPTY, field("condition", DataType.BOOLEAN), List.of(METADATA, fallback));
        rendered = new TimeSeriesToString(Source.EMPTY, conditional, List.of());
        assertEquals("{\"pod\":\"original\"}", evaluate(rendered, true, new BytesRef("{\"pod\":\"original\"}")));
        assertEquals("{\"pod\":\"fallback\"}", evaluate(rendered, false, new BytesRef("{\"pod\":\"original\"}")));
    }

    private void assertValue(DataType type, Object value, String expected) {
        var set = new TimeSeriesSet(Source.EMPTY, METADATA, label("value"), field("value", type));
        String encoded = evaluate(set, null, value);
        assertEquals(expected, evaluate(new TimeSeriesGet(Source.EMPTY, METADATA, label("value")), new BytesRef(encoded)));
        assertEquals(
            encoded,
            evaluate(new TimeSeriesHash(Source.EMPTY, METADATA, List.of(label("value"), field("value", type))), null, value)
        );
    }

    private String evaluate(Expression expression, Object... values) {
        DriverContext context = driverContext();
        Page page = new Page(fromArrayRow(context.blockFactory(), values));
        try (ExpressionEvaluator evaluator = AbstractScalarFunctionTestCase.evaluator(expression).get(context)) {
            try (BytesRefBlock result = (BytesRefBlock) evaluator.eval(page)) {
                assertEquals(1, result.getPositionCount());
                return result.isNull(0) ? null : result.getBytesRef(0, new BytesRef()).utf8ToString();
            }
        } finally {
            page.releaseBlocks();
        }
    }

    private static Literal label(String value) {
        return Literal.keyword(Source.EMPTY, value);
    }

    private static FieldAttribute field(String name, DataType type) {
        return new FieldAttribute(Source.synthetic(name), name, new EsField(name, type, Map.of(), true, EsField.TimeSeriesFieldType.NONE));
    }

    private DriverContext driverContext() {
        return driverContext(ByteSizeValue.ofMb(256));
    }

    private DriverContext driverContext(ByteSizeValue limit) {
        BigArrays arrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, limit).withCircuitBreaking();
        breakers.add(arrays.breakerService().getBreaker(CircuitBreaker.REQUEST));
        return new DriverContext(arrays, BlockFactory.builder(arrays).build(), null);
    }

    /** Failures must release inputs, row scratch, and any partial output. */
    @After
    public void allMemoryReleased() {
        for (CircuitBreaker breaker : breakers) {
            assertEquals(0L, breaker.getUsed());
        }
    }
}
