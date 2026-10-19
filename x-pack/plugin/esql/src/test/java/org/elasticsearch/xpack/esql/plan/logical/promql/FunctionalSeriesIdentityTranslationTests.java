/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.TestAnalyzer;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.aggregate.First;
import org.elasticsearch.xpack.esql.optimizer.rules.PlanConsistencyChecker;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;

public class FunctionalSeriesIdentityTranslationTests extends ESTestCase {
    public void testRawSeriesKeepPhysicalIdentityUntilChecked() {
        for (String expression : List.of(
            "network.bytes_in",
            "abs(network.bytes_in)",
            "rate(network.bytes_in[5m])",
            "network.bytes_in * 2",
            "last_over_time(network.bytes_in[5m])"
        )) {
            LogicalPlan plan = prototype().query(query(expression));
            Failures failures = new Failures();
            plan.forEachUp(node -> PlanConsistencyChecker.checkPlan(node, failures));
            assertFalse(expression + ": " + failures, failures.hasFailures());
            assertTrue(
                expression,
                plan.anyMatch(
                    node -> node instanceof Project project
                        && project.child() instanceof CheckSeriesUniqueness check
                        && check.output().stream().anyMatch(attribute -> attribute.dataType() == DataType.TSID_DATA_TYPE)
                        && project.output().stream().noneMatch(attribute -> attribute.dataType() == DataType.TSID_DATA_TYPE)
                )
            );
            plan.forEachExpressionDown(First.class, first -> fail("Numeric samples retain their existing VALUES aggregate: " + expression));
        }
    }

    public void testAcrossSeriesAggregatesKeepTheirGroupingSemantics() {
        for (String expression : List.of("sum by(cluster,pod)(rate(network.bytes_in[5m]))", "sum without(pod)(network.bytes_in)")) {
            LogicalPlan plan = prototype().query(query(expression));
            assertFalse(expression, plan.anyMatch(node -> node instanceof CheckSeriesUniqueness));
        }
    }

    public void testLegacyRawSeriesDoNotAddIdentityCheck() {
        for (TestAnalyzer legacy : List.of(
            prototype().flags(EsqlFlags.DEFAULTS),
            prototype().minimumTransportVersion(TransportVersionUtils.getPreviousVersion(featureVersion()))
        )) {
            LogicalPlan plan = legacy.query(query("network.bytes_in"));
            assertFalse(plan.anyMatch(node -> node instanceof CheckSeriesUniqueness));
        }
    }

    private static TestAnalyzer prototype() {
        return analyzer().addK8s().minimumTransportVersion(featureVersion()).flags(EsqlFlags.DEFAULTS.withPromqlFunctionalTimeSeries(true));
    }

    private static TransportVersion featureVersion() {
        return TransportVersion.fromName("promql_functional_timeseries");
    }

    private static String query(String expression) {
        return "PROMQL index=k8s start=\"2025-01-01T00:00:00Z\" end=\"2025-01-01T00:01:00Z\" step=1m result=("
            + expression
            + ") | LIMIT 100";
    }
}
