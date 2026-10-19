/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.LoadMapping;
import org.elasticsearch.xpack.esql.SerializationTestUtils;
import org.elasticsearch.xpack.esql.TestAnalyzer;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.analysis.UnmappedResolution;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.expression.TimeSeriesMetadataAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.KeywordEsField;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.esql.expression.function.grouping.TimeSeriesWithout;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.Case;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesGet;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesHash;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesSet;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesToString;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesUnset;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexProperties;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalPlanOptimizer;
import org.elasticsearch.xpack.esql.optimizer.rules.PlanConsistencyChecker;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.join.InnerJoin;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContext.IntermediateResult;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.planner.mapper.Mapper;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.session.Versioned;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.configuration;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.loadUtf8TextFile;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.logicalOptimizerContext;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;

/** Exercises real query analysis with the prototype flag, plus the disabled and mixed-version compatibility paths. */
public class FunctionalTimeSeriesTranslationTests extends ESTestCase {
    public void testDisabledByDefaultIncludingSnapshots() {
        assertFalse(EsqlFlags.ESQL_PROMQL_FUNCTIONAL_TIMESERIES.getDefault(Settings.EMPTY));
        assertFalse(EsqlFlags.DEFAULTS.promqlFunctionalTimeSeries());
        assertTrue(operations(analyzer().addK8s().query(query("sum without(pod)(network.bytes_in)"))).isEmpty());
    }

    public void testMixedVersionUsesWholeLegacyTranslation() {
        LogicalPlan plan = prototype().minimumTransportVersion(previousVersion()).query(query("sum without(pod)(network.bytes_in)"));
        assertTrue(operations(plan).isEmpty());
        assertFalse(plan.collect(EsRelation.class).isEmpty());
        var metadata = new ArrayList<TimeSeriesMetadataAttribute>();
        plan.forEachExpressionDown(TimeSeriesMetadataAttribute.class, metadata::add);
        assertFalse(metadata.isEmpty());
        assertTrue(metadata.stream().allMatch(attribute -> attribute.dataType() == DataType.KEYWORD));
    }

    /** Derived dotted names must bind to their computed label on both legacy paths, rather than become missing grouping keys. */
    public void testLegacyDerivedLabelWithPassthroughPrefix() {
        for (TestAnalyzer analyzer : List.of(
            prototype().flags(EsqlFlags.DEFAULTS),
            prototype().minimumTransportVersion(previousVersion())
        )) {
            for (String relabel : List.of(
                "label_replace(network.bytes_in, \"labels.copy\", \"$1\", \"cluster\", \"(.+)\")",
                "label_join(network.bytes_in, \"labels.copy\", \"\", \"cluster\")"
            )) {
                LogicalPlan plan = analyzer.query(query("sum by(labels.copy)(" + relabel + ")") + " | LIMIT 10");
                assertLegacyGroupingUsesDerivedLabel(plan, "labels.copy");
            }
            LogicalPlan nested = analyzer.query(
                query(
                    "sum by(labels.joined)(label_join("
                        + "label_replace(network.bytes_in, \"labels.copy\", \"$1\", \"cluster\", \"(.+)\"), "
                        + "\"labels.joined\", \"\", \"labels.copy\"))"
                ) + " | LIMIT 10"
            );
            Alias joined = assertLegacyGroupingUsesDerivedLabel(nested, "labels.joined");
            Alias copied = derivedLabel(nested, "labels.copy");
            assertTrue("Nested relabel must read the preceding derived value", joined.references().contains(copied.toAttribute()));
        }
    }

    /** Legacy regrouping must reconnect unpacked physical label names to the command's declared output columns. */
    public void testLegacyPassthroughLabelsSurviveRegrouping() {
        for (String expression : List.of(
            "sum without(pod)(sum by(cluster, region, pod)(network.cost))",
            "sum by(cluster)(sum without(region)(sum by(cluster, region, pod)(network.cost)))",
            "sum by(cluster)(sum without(pod)(sum by(cluster, region, pod)(network.cost)))"
        )) {
            List<String> output = expression.startsWith("sum without")
                ? List.of("result", "step", "cluster", "region")
                : List.of("result", "step", "cluster");
            assertLegacyPassthroughProjection(
                "prometheus-k8s",
                "prometheus-k8s-mappings.json",
                "PROMQL index=prometheus-k8s step=1h result=(" + expression + ") | SORT result | LIMIT 10",
                output
            );
        }
    }

    /** Histogram regrouping also unpacks passthrough labels before sorting and projecting the public result. */
    public void testLegacyPassthroughLabelsSurviveHistogramAggregation() {
        assertLegacyPassthroughProjection(
            "promql_classic_histogram",
            "mapping-promql-classic-histogram-passthrough.json",
            "PROMQL index=promql_classic_histogram step=5m start=\"2024-05-10T00:10:00.000Z\" "
                + "end=\"2024-05-10T00:10:00.000Z\" "
                + "result=(round(histogram_quantile(0.5, sum by(job, le)(request_duration_seconds_bucket)), 0.001)) "
                + "| SORT job | LIMIT 10",
            List.of("result", "step", "job")
        );
    }

    private static void assertLegacyPassthroughProjection(String index, String mapping, String query, List<String> output) {
        for (boolean enabled : List.of(false, true)) {
            TransportVersion version = enabled ? previousVersion() : TransportVersion.current();
            var config = configuration(query);
            LogicalPlan analyzed = analyzer().addIndex(passthroughIndex(index, mapping))
                .minimumTransportVersion(version)
                .flags(EsqlFlags.DEFAULTS.withPromqlFunctionalTimeSeries(enabled))
                .configuration(config)
                .query(query);
            LogicalPlan optimized = new LogicalPlanOptimizer(logicalOptimizerContext(config, FoldContext.small(), version)).optimize(
                analyzed
            );
            assertEquals(query, output, optimized.output().stream().map(Attribute::name).toList());
            Failures failures = new Failures();
            optimized.forEachUp(node -> PlanConsistencyChecker.checkPlan(node, failures));
            assertFalse(query + "\n" + failures, failures.hasFailures());
            assertLegacyMetadata(optimized);
        }
    }

    private static EsIndex passthroughIndex(String name, String mappingFile) {
        // The lightweight mapping loader treats passthrough as unsupported, including all its descendants.
        // Parse its containers as objects before supplying the aliases and inherited dimension markers below.
        String json = loadUtf8TextFile("/index/mappings/" + mappingFile).replace("\"passthrough\"", "\"object\"");
        var mapping = new LinkedHashMap<>(LoadMapping.loadMapping(new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8))));
        // Mapping fixtures do not expand passthrough aliases or inherit dimension markers. Mirror the field-caps
        // result: both the concrete field and its root alias are ordinary fields, with the same dimension type.
        for (String object : List.of("labels", "metrics")) {
            var fields = new LinkedHashMap<String, EsField>();
            for (EsField field : mapping.get(object).getProperties().values()) {
                EsField resolved = object.equals("labels")
                    ? new KeywordEsField(field.getName(), Map.of(), true, 0, false, false, EsField.TimeSeriesFieldType.DIMENSION)
                    : field;
                fields.put(resolved.getName(), resolved);
            }
            mapping.put(object, new EsField(object, DataType.OBJECT, fields, false, EsField.TimeSeriesFieldType.NONE));
            mapping.putAll(fields);
        }
        return new EsIndex(name, mapping, Map.of(name, new IndexProperties(IndexMode.TIME_SERIES, 0)), Map.of(), Map.of());
    }

    /** Disabled and mixed-version queries retain only legacy expressions in their actual data-node wire payloads. */
    public void testLegacyPlansDoNotSerializeTypedMetadata() {
        TransportVersion previous = previousVersion();
        for (TestAnalyzer analyzer : List.of(
            prototype().flags(EsqlFlags.DEFAULTS),
            prototype().minimumTransportVersion(previous),
            prototype().minimumTransportVersion(previous).flags(EsqlFlags.DEFAULTS)
        )) {
            for (String expression : List.of(
                "network.bytes_in",
                "network.bytes_in / network.bytes_in",
                "sum without(pod)(network.bytes_in)",
                "sum by(cluster)(network.bytes_in)",
                "sum by(cluster, pod)(label_replace(sum by(cluster)(network.bytes_in), \"pod\", \"$1\", \"cluster\", \"(.+)\"))"
            )) {
                var config = configuration(query(expression));
                LogicalPlan plan = analyzer.configuration(config).query(query(expression));
                assertLegacyMetadata(plan);
                LogicalPlan optimized = new LogicalPlanOptimizer(logicalOptimizerContext(config, FoldContext.small(), previous)).optimize(
                    plan
                );
                PhysicalPlan physical = new PhysicalPlanOptimizer(new PhysicalOptimizerContext(config, previous)).optimize(
                    new Mapper().map(new Versioned<>(optimized, previous))
                );
                PhysicalPlan dataNodePlan = PlannerUtils.breakPlanBetweenCoordinatorAndDataNode(physical, config).v2();
                assertNotNull(dataNodePlan);
                dataNodePlan.forEachDown(FragmentExec.class, fragment -> assertLegacyMetadata(fragment.fragment()));
                PhysicalPlan copy = SerializationTestUtils.serializeDeserialize(dataNodePlan, (out, value) -> {
                    out.setTransportVersion(previous);
                    out.writeNamedWriteable(value);
                }, in -> {
                    in.setTransportVersion(previous);
                    return in.readNamedWriteable(PhysicalPlan.class);
                }, config);
                assertEquals(dataNodePlan, copy);
                copy.forEachDown(FragmentExec.class, fragment -> assertLegacyMetadata(fragment.fragment()));
            }
        }
    }

    /** Missing indices have no series identity to materialize, including when both operands request it. */
    public void testEmptyIndexDefaultMatchingHasConsistentEmptyPlan() {
        assertEmptyIndexTranslation("foo / foo");
    }

    /** A missing many-side source must produce no rows without constructing aliases for nonexistent metadata. */
    public void testEmptyIndexGroupLeftHasConsistentEmptyPlan() {
        assertEmptyIndexTranslation("foo / on(cluster) group_left sum by(cluster)(foo)");
    }

    /** Explicit time bounds and grouping must not turn an absent source into unresolved output columns or fabricated series. */
    public void testEmptyIndexBoundedExpressionsHaveConsistentEmptyPlans() {
        for (String expression : List.of(
            "foo / foo",
            "foo / on(cluster) group_left sum by(cluster)(foo)",
            "avg(foo) by(job)",
            "1 + foo / foo",
            "absent_over_time(foo[5m])"
        )) {
            assertEmptyIndexTranslation(expression, "start=\"2025-01-01T00:00:00Z\" end=\"2025-01-01T01:00:00Z\" step=5m");
        }
    }

    /** Disabled and older clusters retain their existing constant-vector translation. */
    public void testConstantVectorOperandsPreserveLegacyTranslation() {
        for (TestAnalyzer analyzer : List.of(
            prototype().flags(EsqlFlags.DEFAULTS.withPromqlFunctionalTimeSeries(false)),
            prototype().minimumTransportVersion(previousVersion())
        )) {
            assertLegacyConstantVector(analyzer, "network.bytes_in + vector(1)", List.of("result", "step", MetadataAttribute.TIMESERIES));
            assertLegacyConstantVector(analyzer, "vector(1) + network.bytes_in", List.of("result", "step"));
        }
    }

    public void testMixedVersionRetainsLegacyValidation() {
        assertLegacyValidation(prototype().minimumTransportVersion(previousVersion()));
    }

    /** A new transport version alone must not admit query shapes that require the disabled prototype. */
    public void testDisabledFeatureRetainsLegacyValidation() {
        assertLegacyValidation(prototype().flags(EsqlFlags.DEFAULTS.withPromqlFunctionalTimeSeries(false)));
    }

    public void testRepeatedWithoutReadsOnlyOneSourceIdentity() {
        LogicalPlan plan = prototype().query(query("sum without(cluster)(sum without(pod)(network.bytes_in))"));
        assertThat(operations(plan), hasItem(TimeSeriesUnset.class));
        assertThat(operations(plan), not(hasItem(TimeSeriesHash.class)));
        assertThat(operations(plan), hasItem(TimeSeriesToString.class));
        assertOneMetadataReader(plan);
        for (Aggregate aggregate : plan.collect(Aggregate.class)) {
            assertTrue(aggregate.groupings().stream().filter(group -> group.dataType() == DataType.TIMESERIES_DATA_TYPE).count() <= 1);
        }
        plan.forEachExpressionDown(TimeSeriesWithout.class, e -> fail("functional path must not request source projections"));
    }

    public void testPromotionAfterWithoutReadsTheUpdatedMetadata() {
        LogicalPlan plan = prototype().query(query("sum by(pod)(sum without(pod)(network.bytes_in))"));
        var reads = new ArrayList<TimeSeriesGet>();
        plan.forEachExpressionDown(TimeSeriesGet.class, reads::add);
        assertEquals(1, reads.size());
        // The removed label must be read from the result of WITHOUT, never from the original source dictionary.
        assertFalse(reads.getFirst().children().getFirst() instanceof TimeSeriesMetadataAttribute);
        assertThat(operations(plan), not(hasItem(TimeSeriesToString.class)));
        assertOneMetadataReader(plan);
    }

    public void testDirectByDoesNotReadMetadata() {
        LogicalPlan plan = prototype().query(query("sum by(cluster, pod)(rate(network.bytes_in[5m]))"));
        assertTrue(operations(plan).isEmpty());
        for (EsRelation relation : plan.collect(EsRelation.class)) {
            assertFalse(relation.output().stream().anyMatch(TimeSeriesMetadataAttribute.class::isInstance));
        }
    }

    public void testRfcShapeBecomesFiniteAtBy() {
        LogicalPlan plan = prototype().query(
            query(
                "sum without(disk)(label_replace("
                    + "sum by(cluster, pod)(sum without(host)(rate(network.bytes_in[5m]))),"
                    + " \"disk\", \"$1\", \"pod\", \"^(.+)$\"))"
            )
        );
        assertThat(operations(plan), hasItem(TimeSeriesUnset.class));
        assertThat(operations(plan), not(hasItem(TimeSeriesSet.class)));
        assertThat(operations(plan), not(hasItem(TimeSeriesToString.class)));
        assertOneMetadataReader(plan);
    }

    public void testIgnoringRawLabelsDerivesMatchKeysFromCurrentIdentity() {
        LogicalPlan plan = prototype().query(query("network.bytes_in / ignoring(pod) group_left network.bytes_in"));
        assertEquals(1, plan.collect(InnerJoin.class).size());
        assertThat(operations(plan), hasItem(TimeSeriesUnset.class));
        assertOneMetadataReader(plan);
    }

    public void testCopiedLabelsUseExistingColumnPath() {
        LogicalPlan plan = prototype().query(query("network.bytes_in / on(cluster) group_left(pod) network.bytes_in"));
        InnerJoin join = plan.collect(InnerJoin.class).getFirst();
        var renders = new ArrayList<TimeSeriesToString>();
        plan.forEachExpressionDown(TimeSeriesToString.class, renders::add);
        List<Expression> renderedLabels = renders.getFirst().children();
        Attribute copied = null;
        for (int i = 1; i < renderedLabels.size(); i += 2) {
            if (renderedLabels.get(i) instanceof Literal label && label.value().equals(Literal.keyword(Source.EMPTY, "pod").value())) {
                copied = (Attribute) renderedLabels.get(i + 1);
            }
        }
        assertNotNull(copied);
        Attribute copiedLabel = copied;
        assertTrue(join.right().output().stream().anyMatch(copiedLabel::semanticEquals));
        assertFalse(join.left().output().stream().anyMatch(copiedLabel::semanticEquals));
        var definitions = new ArrayList<Alias>();
        join.right().forEachExpressionDown(Alias.class, definitions::add);
        assertTrue(
            definitions.stream()
                .filter(alias -> alias.id().equals(copiedLabel.id()))
                .anyMatch(
                    alias -> alias.child()
                        .references()
                        .stream()
                        .anyMatch(a -> a instanceof FieldAttribute && PromqlLabels.labelName(a).equals("pod"))
                )
        );
        assertThat(operations(plan), not(hasItem(TimeSeriesSet.class)));
        assertOneMetadataReader(plan);
    }

    public void testFiniteLabelsStayColumnsAfterBy() {
        LogicalPlan plan = prototype().query(
            query("sum without(pod)(label_replace(sum by(cluster, pod)(network.bytes_in)," + " \"pod\", \"$1\", \"cluster\", \"(.+)\"))")
        );
        assertThat(operations(plan), not(hasItem(TimeSeriesSet.class)));
        assertThat(operations(plan), not(hasItem(TimeSeriesToString.class)));
        assertFalse(plan.collect(CheckSeriesUniqueness.class).isEmpty());
        assertOneMetadataReader(plan);
    }

    public void testFiniteLabelNamedTimeSeriesIsNotMetadata() {
        LogicalPlan plan = prototype().query(
            query("sum by(_timeseries)(label_replace(sum by(pod)(network.bytes_in), \"_timeseries\", \"x\", \"pod\", \".*\"))")
        );
        Attribute label = plan.output().stream().filter(a -> a.name().equals(MetadataAttribute.TIMESERIES)).findFirst().orElseThrow();
        assertEquals(DataType.KEYWORD, label.dataType());
        assertFalse(PromqlLabels.isTsMetadata(label));
        assertThat(operations(plan), not(hasItem(TimeSeriesToString.class)));
        for (EsRelation relation : plan.collect(EsRelation.class)) {
            assertFalse(relation.output().stream().anyMatch(TimeSeriesMetadataAttribute.class::isInstance));
        }
    }

    public void testRelabelConstantVectorRetainsItsLabel() {
        LogicalPlan plan = prototype().query(
            "PROMQL start=\"2025-01-01T00:00:00Z\" end=\"2025-01-01T00:02:00Z\" step=1m "
                + "result=(label_join(label_replace(vector(1), \"host\", \"a\", \"missing\", \".*\"), \"zone\", \"\", \"host\") "
                + "or vector(2)) | LIMIT 100"
        );
        assertTrue(plan.output().stream().anyMatch(a -> a.name().equals("host") && a.dataType() == DataType.KEYWORD));
        assertTrue(plan.output().stream().anyMatch(a -> a.name().equals("zone") && a.dataType() == DataType.KEYWORD));
        assertTrue(plan.collect(EsRelation.class).isEmpty());
        assertFalse(plan.collect(CheckSeriesUniqueness.class).isEmpty());
        assertOneMetadataReader(plan);
    }

    public void testSourceFreeCommandsUseTypedTranslationWithoutASetOperator() {
        for (String expression : List.of(
            "1",
            "vector(1)",
            "label_replace(vector(1), \"host\", \"a\", \"\", \"^$\")",
            "scalar(vector(1))"
        )) {
            LogicalPlan plan = prototype().query(
                "PROMQL start=\"2025-01-01T00:00:00Z\" end=\"2025-01-01T00:02:00Z\" step=1m result=(" + expression + ") | LIMIT 100"
            );
            assertTrue(expression, plan.collect(EsRelation.class).isEmpty());
            assertOneMetadataReader(plan);
            if (expression.startsWith("label_replace")) {
                assertEquals(List.of("result", "step", "host"), plan.output().stream().map(Attribute::name).toList());
                assertFalse(plan.collect(CheckSeriesUniqueness.class).isEmpty());
            }
        }
    }

    public void testFunctionsTranslateOverConstantVectors() {
        for (String expression : List.of(
            "count(vector(7))",
            "abs(vector(-1))",
            "sum without(host)(label_replace(vector(1), \"host\", \"a\", \"missing\", \".*\"))",
            "histogram_quantile(0.5, vector(7))"
        )) {
            LogicalPlan plan;
            try {
                plan = prototype().query(
                    "PROMQL start=\"2025-01-01T00:00:00Z\" end=\"2025-01-01T00:02:00Z\" step=1m result=("
                        + expression
                        + " or vector(2)) | LIMIT 100"
                );
            } catch (RuntimeException e) {
                throw new AssertionError(expression, e);
            }
            assertEquals(expression, List.of("result", "step"), plan.output().stream().map(Attribute::name).toList());
            assertTrue(expression, plan.collect(EsRelation.class).isEmpty());
            assertOneMetadataReader(plan);
        }
    }

    public void testRuntimeRelabelPromotesItsInputsAndChecksCollisions() {
        LogicalPlan plan = prototype().query(query("label_replace(network.bytes_in, \"pod\", \"$1\", \"disk\", \"(.+)\")"));
        assertThat(operations(plan), hasItem(TimeSeriesGet.class));
        assertThat(operations(plan), hasItem(TimeSeriesUnset.class));
        assertThat(operations(plan), not(hasItem(TimeSeriesSet.class)));
        assertLabelRewriteIsChecked(plan, "label_replace", "pod");
        assertOneMetadataReader(plan);
    }

    public void testEmptyReplaceSourceDoesNotReadMetadata() {
        LogicalPlan plan = prototype().query(query("label_replace(network.bytes_in, \"pod\", \"empty\", \"\", \"^$\")"));
        var gets = new ArrayList<TimeSeriesGet>();
        plan.forEachExpressionDown(TimeSeriesGet.class, gets::add);
        for (TimeSeriesGet get : gets) {
            assertFalse(get.children().contains(Literal.keyword(get.source(), "")));
        }
        assertOneMetadataReader(plan);
    }

    public void testKnownColumnsDoNotReadMetadata() {
        for (String expression : List.of(
            "sum by(cluster, pod)(rate(network.bytes_in[5m]))",
            "sum by(cluster)(network.bytes_in) / 2",
            "sum by(cluster)(network.bytes_in) / on(cluster) sum by(cluster)(network.bytes_in)"
        )) {
            LogicalPlan existing = analyzer().addK8s()
                .minimumTransportVersion(TransportVersion.fromName("promql_functional_timeseries"))
                .query(query(expression));
            LogicalPlan prototype = prototype().query(query(expression));
            // Promoted labels stay columns. Empty-label normalization and join cardinality checks
            // may add expressions, but must not introduce a source metadata read or change the output schema.
            assertEquals(
                existing.output().stream().map(Attribute::name).toList(),
                prototype.output().stream().map(Attribute::name).toList()
            );
            assertEquals(
                existing.output().stream().map(Attribute::dataType).toList(),
                prototype.output().stream().map(Attribute::dataType).toList()
            );
            for (EsRelation relation : prototype.collect(EsRelation.class)) {
                assertFalse(expression, relation.output().stream().anyMatch(TimeSeriesMetadataAttribute.class::isInstance));
            }
        }
    }

    public void testDefaultRawMatchingUsesExistingJoinAssembly() {
        LogicalPlan plan = prototype().query(query("network.bytes_in / network.bytes_in"));
        assertEquals(1, plan.collect(InnerJoin.class).size());
        assertTrue(plan.collect(InnerJoin.class).getFirst().unique());
        assertThat(operations(plan), hasItem(TimeSeriesHash.class));
        assertOneMetadataReader(plan);
    }

    public void testNestedRawMatchingKeepsIndependentInputs() {
        LogicalPlan plan = prototype().query(
            query("sum by(cluster)(network.bytes_in / network.bytes_in)" + " / sum by(cluster)(network.bytes_in / network.bytes_in)")
        );
        assertEquals(3, plan.collect(InnerJoin.class).size());
        assertOneMetadataReader(plan);
    }

    public void testNestedScalarComparisonFiltersPerStepValues() {
        for (String expression : List.of("(network.bytes_in > 0) * 2", "(0 < network.bytes_in) * 2")) {
            LogicalPlan plan = prototype().query(query(expression));
            assertTrue(
                plan.collect(Filter.class)
                    .stream()
                    .anyMatch(filter -> filter.condition() instanceof GreaterThan || filter.condition() instanceof LessThan)
            );
            assertOneMetadataReader(plan);
        }
    }

    public void testNewLabelSurvivesFiniteRelabel() {
        LogicalPlan plan = prototype().query(
            query("label_replace(sum by(cluster)(network.bytes_in), \"disk\", \"$1\", \"cluster\", \"(.+)\")")
        );
        assertThat(plan.output().stream().map(a -> a.name()).toList(), hasItem("disk"));
        assertThat(operations(plan), not(hasItem(TimeSeriesSet.class)));
        assertThat(operations(plan), not(hasItem(TimeSeriesToString.class)));
    }

    public void testGroupRightBuildsFromTheOneSide() {
        LogicalPlan plan = prototype().query(query("sum by(cluster)(network.bytes_in) / on(cluster) group_right network.bytes_in"));
        InnerJoin join = plan.collect(InnerJoin.class).getFirst();
        var buildSums = new ArrayList<Sum>();
        join.right().forEachExpressionDown(Sum.class, buildSums::add);
        assertFalse(buildSums.isEmpty());
        join.left().forEachExpressionDown(Sum.class, sum -> fail("the aggregate is the build input"));
        assertFalse(join.unique());
        assertOneMetadataReader(plan);
    }

    public void testNestedJoinsPreserveCurrentMetadata() {
        LogicalPlan plan = prototype().query(
            query(
                "(network.bytes_in / on(cluster) group_left sum by(cluster)(network.bytes_in))"
                    + " / on(cluster) group_left sum by(cluster)(network.bytes_in)"
            )
        );
        assertEquals(2, plan.collect(InnerJoin.class).size());
        assertOneMetadataReader(plan);
    }

    public void testDefaultRawToFiniteMatchUsesFullIdentity() {
        LogicalPlan plan = prototype().query(query("network.bytes_in / sum by(cluster)(network.bytes_in)"));
        assertEquals(1, plan.collect(InnerJoin.class).size());
        assertThat(operations(plan), hasItem(TimeSeriesHash.class));
        assertEquals(List.of("result", "step", MetadataAttribute.TIMESERIES), plan.output().stream().map(Attribute::name).toList());
        assertThat(plan.toString(), not(containsString("{}")));
        assertOneMetadataReader(plan);
    }

    /** Full-identity matching must keep the left operand's labels when only the right side has a dictionary. */
    public void testDefaultFiniteToRawMatchKeepsFiniteIdentity() {
        LogicalPlan plan = prototype().query(query("sum by(cluster)(network.bytes_in) / network.bytes_in"));
        assertEquals(1, plan.collect(InnerJoin.class).size());
        assertThat(operations(plan), hasItem(TimeSeriesHash.class));
        assertEquals(List.of("result", "step", "cluster"), plan.output().stream().map(Attribute::name).toList());
        assertOneMetadataReader(plan);
    }

    public void testOnGroupLeftKeepsManySideIdentity() {
        LogicalPlan plan = prototype().query(query("network.bytes_in / on(cluster) group_left sum by(cluster)(network.bytes_in)"));
        assertEquals(1, plan.collect(InnerJoin.class).size());
        assertThat(operations(plan), hasItem(TimeSeriesToString.class));
        assertOneMetadataReader(plan);
    }

    public void testValueFunctionsAndScalarArithmeticKeepTypedLabels() {
        LogicalPlan plan = prototype().query(query("ceil(rate(network.bytes_in[5m])) / 2"));
        assertOneMetadataReader(plan);
        assertThat(operations(plan), hasItem(TimeSeriesToString.class));
    }

    public void testComparisonFilterKeepsTypedLabels() {
        LogicalPlan plan = prototype().query(query("network.bytes_in > 0"));
        assertOneMetadataReader(plan);
        assertThat(operations(plan), hasItem(TimeSeriesToString.class));
    }

    public void testVectorComparisonPreservesUnchangedMetadata() {
        LogicalPlan plan = prototype().query(query("network.bytes_in > network.bytes_in"));
        InnerJoin join = plan.collect(InnerJoin.class).getFirst();
        Attribute metadata = join.left()
            .output()
            .stream()
            .filter(attribute -> attribute.dataType() == DataType.TIMESERIES_DATA_TYPE)
            .findFirst()
            .orElseThrow();
        var aliases = new ArrayList<Alias>();
        plan.forEachExpressionDown(Alias.class, aliases::add);
        assertTrue(aliases.stream().anyMatch(alias -> alias.child().semanticEquals(metadata)));
        assertOneMetadataReader(plan);
    }

    public void testClassicHistogramUsesCurrentMetadata() {
        LogicalPlan plan = prototype().query(query("histogram_quantile(0.9, rate(network.bytes_in[5m]))"));
        assertOneMetadataReader(plan);
        assertThat(operations(plan), hasItem(TimeSeriesGet.class));
        assertThat(operations(plan), hasItem(TimeSeriesUnset.class));
    }

    public void testChainedRelabelsReadUpdatedColumnsAndMetadata() {
        for (String input : List.of("network.bytes_in", "sum by(cluster)(network.bytes_in)")) {
            LogicalPlan plan = prototype().query(
                query(
                    "label_join(label_replace("
                        + input
                        + ", \"cluster\", \"$1\", \"pod\", \"(.+)\"), "
                        + "\"disk\", \"-\", \"cluster\", \"pod\")"
                )
            );
            assertLabelRewriteIsChecked(plan, "label_replace", "cluster");
            assertLabelRewriteIsChecked(plan, "label_join", "disk");
            assertOneMetadataReader(plan);
        }
    }

    public void testGroupRightCopiedLabelsSurviveNestedJoin() {
        LogicalPlan plan = prototype().query(
            query(
                "(sum by(cluster, pod)(network.bytes_in) / on(cluster) group_right(pod) network.bytes_in)"
                    + " / on(cluster) group_left sum by(cluster)(network.bytes_in)"
            )
        );
        assertEquals(2, plan.collect(InnerJoin.class).size());
        assertOneMetadataReader(plan);
    }

    public void testOneToOneOnMaterializesRuntimeKey() {
        LogicalPlan plan = prototype().query(query("network.bytes_in / on(disk) network.bytes_in"));
        assertTrue(plan.collect(InnerJoin.class).getFirst().unique());
        assertThat(operations(plan), hasItem(TimeSeriesGet.class));
        assertOneMetadataReader(plan);
    }

    public void testDroppedLabelDoesNotBindToTemporaryColumn() {
        var label = new ReferenceAttribute(Source.EMPTY, "pod", DataType.KEYWORD);
        var plan = new LocalRelation(Source.EMPTY, List.of(label), EmptyLocalSupplier.EMPTY);
        var result = new IntermediateResult(plan, TranslationSchema.EMPTY, Literal.NULL, label);
        assertNull(result.metadata());
        assertNull(result.labelColumn("pod"));
    }

    public void testLogicalLabelsDoNotBindExecutionColumnsByName() {
        var step = new ReferenceAttribute(Source.EMPTY, "step", DataType.DATETIME);
        var value = new ReferenceAttribute(Source.EMPTY, "result", DataType.DOUBLE);
        var label = new ReferenceAttribute(Source.EMPTY, "internal_label", DataType.KEYWORD);
        var plan = new LocalRelation(Source.EMPTY, List.of(step, value, label), EmptyLocalSupplier.EMPTY);
        var result = new IntermediateResult(
            plan,
            TranslationSchema.finite(List.of("result")),
            value,
            step,
            null,
            IntermediateResult.Kind.AFTER_INITIAL_AGGREGATE,
            Map.of("result", label)
        );
        assertSame(label, result.labelColumn("result"));
        assertNull(result.label("step"));
        Attribute renamed = label.withName("another_internal_name");
        var renamedPlan = new LocalRelation(Source.EMPTY, List.of(step, value, renamed), EmptyLocalSupplier.EMPTY);
        assertSame(renamed, result.with(renamedPlan, result.schema(), value).labelColumn("result"));

        var unrelated = new ReferenceAttribute(Source.EMPTY, label.name(), DataType.KEYWORD);
        var unrelatedPlan = new LocalRelation(Source.EMPTY, List.of(step, value, unrelated), EmptyLocalSupplier.EMPTY);
        var error = expectThrows(IllegalStateException.class, () -> result.with(unrelatedPlan, result.schema(), value));
        assertEquals("Missing column for promoted label [result]", error.getMessage());
    }

    public void testUniquenessIgnoresAbsentSamples() {
        var step = new ReferenceAttribute(Source.EMPTY, "step", DataType.DATETIME);
        var value = new ReferenceAttribute(Source.EMPTY, "value", DataType.DOUBLE);
        var plan = new LocalRelation(Source.EMPTY, List.of(step, value), EmptyLocalSupplier.EMPTY);
        var input = new IntermediateResult(plan, TranslationSchema.EMPTY, value, step);
        var result = PromqlPlan.checkUnique(Source.EMPTY, input);
        CheckSeriesUniqueness check = (CheckSeriesUniqueness) result.plan();
        Filter filter = (Filter) check.child();
        assertSame(value, ((IsNotNull) filter.condition()).field());
        assertSame(plan, filter.child());
        assertSame(step, check.step());
        assertEquals(plan.output(), check.output());
    }

    public void testOpenSchemaRequiresExactlyOneMetadataColumn() {
        var first = new ReferenceAttribute(Source.EMPTY, "first", DataType.TIMESERIES_DATA_TYPE);
        var second = new ReferenceAttribute(Source.EMPTY, "second", DataType.TIMESERIES_DATA_TYPE);
        var step = new ReferenceAttribute(Source.EMPTY, "step", DataType.DATETIME);
        for (var columns : List.of(List.<Attribute>of(step), List.<Attribute>of(step, first, second))) {
            var plan = new LocalRelation(Source.EMPTY, columns, EmptyLocalSupplier.EMPTY);
            var result = new IntermediateResult(plan, TranslationSchema.open(), Literal.NULL, step);
            var error = expectThrows(IllegalStateException.class, result::metadata);
            assertThat(error.getMessage(), containsString("Expected one time-series metadata column"));
        }
        var plan = new LocalRelation(Source.EMPTY, List.of(step, first), EmptyLocalSupplier.EMPTY);
        var result = new IntermediateResult(plan, TranslationSchema.open(), Literal.NULL, step);
        assertSame(first, result.metadata());
    }

    public void testPromotedLabelRequiresItsColumn() {
        var step = new ReferenceAttribute(Source.EMPTY, "step", DataType.DATETIME);
        var plan = new LocalRelation(Source.EMPTY, List.of(step), EmptyLocalSupplier.EMPTY);
        var result = new IntermediateResult(plan, TranslationSchema.finite(List.of("pod")), Literal.NULL, step);
        var error = expectThrows(IllegalStateException.class, () -> result.labelColumn("pod"));
        assertEquals("Missing column for promoted label [pod]", error.getMessage());
    }

    private static TestAnalyzer prototype() {
        return analyzer().addK8s()
            .minimumTransportVersion(TransportVersion.fromName("promql_functional_timeseries"))
            .flags(EsqlFlags.DEFAULTS.withPromqlFunctionalTimeSeries(true));
    }

    private static TransportVersion previousVersion() {
        return TransportVersionUtils.getPreviousVersion(TransportVersion.fromName("promql_functional_timeseries"));
    }

    private void assertEmptyIndexTranslation(String expression) {
        assertEmptyIndexTranslation(expression, "step=5m");
    }

    private void assertEmptyIndexTranslation(String expression, String timeRange) {
        LogicalPlan plan = analyzer().addEmptyIndex()
            .unmappedResolution(UnmappedResolution.NULLIFY)
            .minimumTransportVersion(TransportVersion.fromName("promql_functional_timeseries"))
            .flags(EsqlFlags.DEFAULTS.withPromqlFunctionalTimeSeries(true))
            .query("PROMQL index=empty_index " + timeRange + " result=(" + expression + ")");
        assertWarnings("No limit defined, adding default limit of [1000]");
        assertTrue(expression, plan.output().stream().allMatch(Attribute::resolved));
        assertTrue(expression, plan.collect(EsRelation.class).isEmpty());
        var relations = plan.collect(LocalRelation.class);
        assertFalse(expression, relations.isEmpty());
        for (LocalRelation relation : relations) {
            assertSame(expression, EmptyLocalSupplier.EMPTY, relation.supplier());
        }
        assertOneMetadataReader(plan);
    }

    private static void assertLegacyConstantVector(TestAnalyzer analyzer, String expression, List<String> output) {
        LogicalPlan plan = analyzer.query(query(expression));
        assertTrue(expression, plan.collect(InnerJoin.class).isEmpty());
        assertTrue(expression, operations(plan).isEmpty());
        assertEquals(expression, output, plan.output().stream().map(Attribute::name).toList());
        Failures failures = new Failures();
        plan.forEachUp(node -> PlanConsistencyChecker.checkPlan(node, failures));
        assertFalse(failures.toString(), failures.hasFailures());
    }

    private static void assertLegacyValidation(TestAnalyzer analyzer) {
        for (var scenario : List.of(
            List.of("sum without(cluster)(sum without(pod)(network.bytes_in))", "nested WITHOUT over WITHOUT"),
            List.of(
                "network.bytes_in / on(cluster) group_left sum by(cluster)(network.bytes_in)",
                "vector matching requires operands with concrete label sets"
            ),
            List.of(
                "network.bytes_in / ignoring(pod) group_left network.bytes_in",
                "vector matching requires operands with concrete label sets"
            ),
            List.of(
                "network.bytes_in / sum by(cluster)(network.bytes_in)",
                "binary operations between an aggregated and a raw vector are not supported"
            ),
            List.of("(network.bytes_in > 0) * 2", "comparison operators are only supported at the top-level"),
            List.of(
                "label_replace(sum by(cluster)(network.bytes_in), \"pod\", \"$1\", \"cluster\", \"(.+)\")",
                "[label_replace] is only supported inside a `by(...)` aggregation, but was used as a top-level (non-aggregated) expression"
            )
        )) {
            var error = expectThrows(VerificationException.class, () -> analyzer.query(query(scenario.getFirst())));
            assertThat(scenario.getFirst(), error.getMessage(), containsString(scenario.getLast()));
        }
    }

    private static void assertLabelRewriteIsChecked(LogicalPlan plan, String function, String label) {
        var derived = plan.collect(Eval.class)
            .stream()
            .flatMap(eval -> eval.fields().stream())
            .filter(alias -> alias.child() instanceof Case && alias.source().text().startsWith(function + "("))
            .toList();
        assertFalse("Missing derived label for " + function, derived.isEmpty());
        for (Alias alias : derived) {
            assertTrue(
                "The collision key must read the new value of " + label,
                plan.collect(CheckSeriesUniqueness.class).stream().anyMatch(check -> {
                    List<Expression> key = check.identity().children();
                    for (int i = 1; i + 1 < key.size(); i += 2) {
                        if (key.get(i).equals(Literal.keyword(key.get(i).source(), label))
                            && key.get(i + 1).semanticEquals(alias.toAttribute())) {
                            return true;
                        }
                    }
                    return false;
                })
            );
        }
    }

    private static Alias assertLegacyGroupingUsesDerivedLabel(LogicalPlan plan, String name) {
        Failures failures = new Failures();
        plan.forEachUp(node -> PlanConsistencyChecker.checkPlan(node, failures));
        assertFalse(failures.toString(), failures.hasFailures());
        assertLegacyMetadata(plan);
        Alias derived = derivedLabel(plan, name);
        assertTrue(
            "Grouping must use the computed label: " + plan,
            plan.collect(Aggregate.class)
                .stream()
                .flatMap(aggregate -> aggregate.groupings().stream())
                .anyMatch(group -> Alias.unwrap(group).semanticEquals(derived.toAttribute()))
        );
        return derived;
    }

    private static Alias derivedLabel(LogicalPlan plan, String name) {
        return plan.collect(Eval.class)
            .stream()
            .flatMap(eval -> eval.fields().stream())
            .filter(
                alias -> alias.name().equals(name)
                    && alias.child() instanceof Attribute == false
                    && alias.child() instanceof Literal == false
            )
            .findFirst()
            .orElseThrow();
    }

    private static String query(String expression) {
        return "PROMQL index=k8s step=1h result=(" + expression + ")";
    }

    private static List<Class<? extends TimeSeriesFunction>> operations(LogicalPlan plan) {
        var operations = new ArrayList<Class<? extends TimeSeriesFunction>>();
        plan.forEachExpressionDown(TimeSeriesFunction.class, function -> operations.add(function.getClass()));
        return operations;
    }

    private static void assertLegacyMetadata(LogicalPlan plan) {
        assertTrue(operations(plan).isEmpty());
        assertTrue(plan.collect(CheckSeriesUniqueness.class).isEmpty());
        plan.forEachExpressionDown(Expression.class, expression -> assertNotEquals(DataType.TIMESERIES_DATA_TYPE, expression.dataType()));
        plan.forEachDown(node -> {
            for (Attribute attribute : node.output()) {
                assertNotEquals(DataType.TIMESERIES_DATA_TYPE, attribute.dataType());
            }
        });
    }

    private static void assertOneMetadataReader(LogicalPlan plan) {
        Failures failures = new Failures();
        plan.forEachUp(node -> PlanConsistencyChecker.checkPlan(node, failures));
        assertFalse(failures.toString(), failures.hasFailures());
        plan.forEachUp(
            node -> assertTrue(
                node.nodeName() + " exposes more than one metadata value",
                node.output().stream().filter(attribute -> attribute.dataType() == DataType.TIMESERIES_DATA_TYPE).count() <= 1
            )
        );
        plan.forEachExpressionDown(TimeSeriesFunction.class, function -> {
            assertTrue(function.typeResolved().toString(), function.typeResolved().resolved());
            assertEquals(DataType.TIMESERIES_DATA_TYPE, function.children().getFirst().dataType());
        });
        for (EsRelation relation : plan.collect(EsRelation.class)) {
            relation.output()
                .stream()
                .filter(TimeSeriesMetadataAttribute.class::isInstance)
                .forEach(attribute -> assertEquals(DataType.TIMESERIES_DATA_TYPE, attribute.dataType()));
            assertTrue(relation.output().stream().filter(TimeSeriesMetadataAttribute.class::isInstance).count() <= 1);
        }
    }
}
