/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionDefinition;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionRegistry;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.Evaluation;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.InstantSelector;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.LabelMatcher;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.LabelMatchers;

import java.util.List;
import java.util.Map;

public class PromqlLabelTypeTests extends ESTestCase {
    public void testEmptyReplaceSourceIsNotARequiredMetadataKey() {
        var relabel = new MetadataManipulationFunction(
            Source.EMPTY,
            raw(),
            definition("label_replace"),
            List.of(
                Literal.keyword(Source.EMPTY, "host"),
                Literal.keyword(Source.EMPTY, "empty"),
                Literal.keyword(Source.EMPTY, ""),
                Literal.keyword(Source.EMPTY, "^$")
            )
        );
        assertEquals(List.of("host"), relabel.sourceLabels());
    }

    public void testHiddenRewriteFactsDoNotAddOutputColumns() {
        InstantSelector raw = raw();
        MetadataManipulationFunction relabel = relabel(raw, "rack");
        assertEquals(raw.output(), relabel.output());
        assertEquals(Map.of("rack", DataType.KEYWORD), relabel.labelTypes());
        expectThrows(UnsupportedOperationException.class, () -> relabel.labelTypes().put("host", DataType.LONG));

        var abs = new ValueTransformationFunction(Source.EMPTY, relabel, definition("abs"), List.of());
        assertEquals(raw.output(), abs.output());
        assertEquals(relabel.labelTypes(), abs.labelTypes());
    }

    public void testAggregationBoundariesRemoveFacts() {
        MetadataManipulationFunction relabel = relabel(raw(), "rack");
        var host = new ReferenceAttribute(Source.EMPTY, "host", DataType.LONG);
        var rack = new ReferenceAttribute(Source.EMPTY, "rack", DataType.KEYWORD);
        assertEquals(
            Map.of("host", DataType.LONG),
            new AcrossSeriesAggregate(Source.EMPTY, relabel, definition("sum"), List.of(), AcrossSeriesAggregate.Grouping.BY, List.of(host))
                .labelTypes()
        );
        assertEquals(
            Map.of(),
            new AcrossSeriesAggregate(Source.EMPTY, relabel, definition("sum"), List.of(), AcrossSeriesAggregate.Grouping.NONE, List.of())
                .labelTypes()
        );
        assertEquals(
            Map.of(),
            new AcrossSeriesAggregate(
                Source.EMPTY,
                relabel,
                definition("sum"),
                List.of(),
                AcrossSeriesAggregate.Grouping.WITHOUT,
                List.of(rack)
            ).labelTypes()
        );
        assertEquals(
            relabel.labelTypes(),
            new AcrossSeriesAggregate(
                Source.EMPTY,
                relabel,
                definition("sum"),
                List.of(),
                AcrossSeriesAggregate.Grouping.WITHOUT,
                List.of(host)
            ).labelTypes()
        );
        assertEquals(Map.of(), new ScalarConversionFunction(Source.EMPTY, relabel, definition("scalar"), List.of()).labelTypes());
    }

    public void testHistogramRemovesBucketAndMetricNameFacts() {
        var child = relabel(relabel(relabel(raw(), "rack"), HistogramFunctionCall.LE_LABEL), LabelMatcher.NAME);
        var histogram = new HistogramQuantile(
            Source.EMPTY,
            child,
            definition("histogram_quantile"),
            List.of(Literal.fromDouble(Source.EMPTY, 0.5))
        );
        assertEquals(Map.of("rack", DataType.KEYWORD), histogram.labelTypes());
        assertTrue(PromqlLabels.hasTsMetadata(histogram.output()));
    }

    private static InstantSelector raw() {
        return new InstantSelector(
            Source.EMPTY,
            new ReferenceAttribute(Source.EMPTY, "metric", DataType.DOUBLE),
            List.of(),
            LabelMatchers.EMPTY,
            Evaluation.NONE
        );
    }

    private static MetadataManipulationFunction relabel(LogicalPlan child, String destination) {
        return new MetadataManipulationFunction(
            Source.EMPTY,
            child,
            definition("label_replace"),
            List.of(
                Literal.keyword(Source.EMPTY, destination),
                Literal.keyword(Source.EMPTY, "replacement"),
                Literal.keyword(Source.EMPTY, "source"),
                Literal.keyword(Source.EMPTY, ".*")
            )
        );
    }

    private static PromqlFunctionDefinition definition(String name) {
        return PromqlFunctionRegistry.INSTANCE.functionMetadata(name);
    }
}
