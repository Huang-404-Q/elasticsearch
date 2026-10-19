/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.analytics.AnalyticsPlugin;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.LabelMatcher;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;

/** Executes functional metadata plans across data nodes, including source conversion and promoted label boundaries. */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 2, numClientNodes = 0, supportsDedicatedMasters = false)
public class FunctionalTimeSeriesIT extends AbstractEsqlIntegTestCase {
    private static final String INDEX = "metrics-functional";
    private static final String QUERY_TIME = "2024-05-10T00:01:00Z";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), AnalyticsPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(EsqlFlags.ESQL_PROMQL_FUNCTIONAL_TIMESERIES.getKey(), true)
            .build();
    }

    @Override
    protected QueryPragmas getPragmas() {
        // Small pages exercise metadata ownership and duplicate detection across page boundaries.
        return canUseQueryPragmas() ? new QueryPragmas(Settings.builder().put("page_size", 1).build()) : super.getPragmas();
    }

    public void testMissingSourceIndicesPreserveEmptyVectorsAndScalarClock() {
        String missing = "metrics-nonexistent-*";
        assertRows(query(missing, "tx", QUERY_TIME) + " | KEEP result, _timeseries", List.of());
        assertRows(
            query(missing, "scalar(tx)", "2024-05-10T00:03:00Z") + " | KEEP result",
            List.of(List.of(Double.NaN), List.of(Double.NaN), List.of(Double.NaN))
        );
        assertRows(query(missing, "vector(1)", QUERY_TIME) + " | KEEP result", List.of(List.of(1.0)));
    }

    public void testRawLabelsAndRewrites() {
        createMetricsIndex(INDEX);
        index(INDEX, "cluster", "prod", "host", "a", "tx", 10.0);
        index(INDEX, "cluster", "prod", "host", "b", "tx", 20.0);
        refresh(INDEX);

        assertRows(
            query("tx") + " | KEEP result, _timeseries",
            List.of(List.of(10.0, "{\"cluster\":\"prod\",\"host\":\"a\"}"), List.of(20.0, "{\"cluster\":\"prod\",\"host\":\"b\"}"))
        );
        assertRows(
            query("label_replace(tx, \"zone\", \"$1\", \"host\", \"(.+)\")") + " | KEEP result, _timeseries",
            List.of(
                List.of(10.0, "{\"cluster\":\"prod\",\"host\":\"a\",\"zone\":\"a\"}"),
                List.of(20.0, "{\"cluster\":\"prod\",\"host\":\"b\",\"zone\":\"b\"}")
            )
        );
        assertRows(
            query("label_join(tx, \"joined\", \"/\", \"cluster\", \"host\")") + " | KEEP result, _timeseries",
            List.of(
                List.of(10.0, "{\"cluster\":\"prod\",\"host\":\"a\",\"joined\":\"prod/a\"}"),
                List.of(20.0, "{\"cluster\":\"prod\",\"host\":\"b\",\"joined\":\"prod/b\"}")
            )
        );
    }

    public void testNestedRelabelGrouping() {
        createMetricsIndex(INDEX);
        index(INDEX, "cluster", "prod", "host", "a", "tx", 10.0);
        index(INDEX, "cluster", "prod", "host", "b", "tx", 20.0);
        refresh(INDEX);

        assertRows(
            query("sum without(host) (label_replace(tx, \"zone\", \"$1\", \"cluster\", \"(.+)\"))") + " | KEEP result, _timeseries",
            List.of(List.of(30.0, "{\"cluster\":\"prod\",\"zone\":\"prod\"}"))
        );
        assertRows(
            query("sum by(zone) (label_replace(tx, \"zone\", \"$1\", \"cluster\", \"(.+)\"))") + " | KEEP result, zone",
            List.of(List.of(30.0, "prod"))
        );
        assertRows(
            query("sum without(host) (label_join(tx, \"zone\", \"/\", \"cluster\"))") + " | KEEP result, _timeseries",
            List.of(List.of(30.0, "{\"cluster\":\"prod\",\"zone\":\"prod\"}"))
        );
        assertRows(
            query("sum by(joined) (label_join(tx, \"joined\", \"/\", \"cluster\", \"host\"))") + " | KEEP result, joined",
            List.of(List.of(10.0, "prod/a"), List.of(20.0, "prod/b"))
        );
    }

    public void testRawGroupLeftCopiesLabels() {
        createMetricsIndex(INDEX);
        index(INDEX, "cluster", "prod", "host", "a", "tx", 10.0);
        index(INDEX, "cluster", "prod", "host", "b", "tx", 20.0);
        index(INDEX, "cluster", "prod", "region", "eu", "identity", 2.0);
        refresh(INDEX);

        assertRows(
            query("tx / on(cluster) group_left(region) identity") + " | KEEP result, _timeseries",
            List.of(
                List.of(5.0, "{\"cluster\":\"prod\",\"host\":\"a\",\"region\":\"eu\"}"),
                List.of(10.0, "{\"cluster\":\"prod\",\"host\":\"b\",\"region\":\"eu\"}")
            )
        );
        assertRows(
            query("sum without(host) (tx / on(cluster) group_left(region) identity)") + " | KEEP result, _timeseries",
            List.of(List.of(15.0, "{\"cluster\":\"prod\",\"region\":\"eu\"}"))
        );
        assertRows(
            query("identity / on(cluster) group_right(region) tx") + " | KEEP result, _timeseries",
            List.of(
                List.of(0.2, "{\"cluster\":\"prod\",\"host\":\"a\",\"region\":\"eu\"}"),
                List.of(0.1, "{\"cluster\":\"prod\",\"host\":\"b\",\"region\":\"eu\"}")
            )
        );
    }

    public void testComparisonsPreserveVectorValuesAndLabels() {
        createMetricsIndex(INDEX);
        index(INDEX, "cluster", "prod", "host", "a", "tx", 10.0, "rx", 2.0);
        index(INDEX, "cluster", "prod", "host", "b", "tx", 20.0, "rx", 25.0);
        index(INDEX, "cluster", "prod", "region", "eu", "identity", 15.0);
        refresh(INDEX);

        assertRows(query("tx > rx") + " | KEEP result, _timeseries", List.of(List.of(10.0, "{\"cluster\":\"prod\",\"host\":\"a\"}")));
        assertRows(
            query("tx > bool rx") + " | KEEP result, _timeseries",
            List.of(List.of(1.0, "{\"cluster\":\"prod\",\"host\":\"a\"}"), List.of(0.0, "{\"cluster\":\"prod\",\"host\":\"b\"}"))
        );
        assertRows(query("sum(tx > rx)") + " | KEEP result", List.of(List.of(10.0)));
        assertRows(
            query("tx > on(cluster) group_left(region) identity") + " | KEEP result, _timeseries",
            List.of(List.of(20.0, "{\"cluster\":\"prod\",\"host\":\"b\",\"region\":\"eu\"}"))
        );
        assertRows(query("15 < tx") + " | KEEP result, _timeseries", List.of(List.of(20.0, "{\"cluster\":\"prod\",\"host\":\"b\"}")));
        assertRows(
            query("15 < bool tx") + " | KEEP result, _timeseries",
            List.of(List.of(0.0, "{\"cluster\":\"prod\",\"host\":\"a\"}"), List.of(1.0, "{\"cluster\":\"prod\",\"host\":\"b\"}"))
        );
    }

    public void testCopiedLabelsUseBuildSideTypes() {
        createMetricsIndex(INDEX);
        index(INDEX, "cluster", "prod", "host", "a", "shard", 1L, "tx", 10.0);
        index(INDEX, "cluster", "prod", "host", "b", "shard", 2L, "rx", 2.0);
        refresh(INDEX);

        String one = "label_replace(sum by(cluster) (rx), \"shard\", \"x\", \"missing\", \".*\")";
        String many = "sum by(cluster, shard) (tx)";
        for (String expression : List.of(
            many + " / on(cluster) group_left(shard) " + one,
            one + " / on(cluster) group_right(shard) " + many
        )) {
            double expected = expression.startsWith(many) ? 5.0 : 0.2;
            assertRows(
                query(expression) + " | EVAL decorated = CONCAT(shard, \"!\") | KEEP result, decorated",
                List.of(List.of(expected, "x!"))
            );
        }
    }

    public void testScalarCountsSeriesAndBroadcastsPerStep() {
        createMetricsIndex(INDEX);
        index(INDEX, "cluster", "prod", "host", "a", "tx", 10.0, "rx", 2.0);
        index(INDEX, "cluster", "prod", "host", "b", "tx", 20.0, "rx", 2.0);
        refresh(INDEX);

        assertRows(query("scalar(rx)") + " | KEEP result", List.of(List.of(Double.NaN)));
        assertRows(query("scalar(tx{host=\"a\"})") + " | KEEP result", List.of(List.of(10.0)));
        assertRows(
            query("tx / scalar(sum(rx))") + " | KEEP result, _timeseries",
            List.of(List.of(2.5, "{\"cluster\":\"prod\",\"host\":\"a\"}"), List.of(5.0, "{\"cluster\":\"prod\",\"host\":\"b\"}"))
        );
        assertRows(query("scalar(sum(rx)) / tx") + " | KEEP result", List.of(List.of(0.4), List.of(0.2)));
        assertRows(query("sum(tx) / scalar(count(count by(host) (tx)))") + " | KEEP result", List.of(List.of(15.0)));
        assertRows(query("tx > scalar(rx)") + " | KEEP result", List.of());
        assertRows(query("scalar(rx) < tx") + " | KEEP result", List.of());
        assertRows(query("tx > bool scalar(rx)") + " | KEEP result", List.of(List.of(0.0), List.of(0.0)));
        assertRows(query("scalar(sum(rx)) < tx") + " | KEEP result", List.of(List.of(10.0), List.of(20.0)));
    }

    public void testScalarFillsMissingEvaluationSteps() {
        createMetricsIndex(INDEX);
        index(INDEX, "cluster", "prod", "host", "a", "tx", 10.0);
        refresh(INDEX);

        assertRows(
            query(INDEX, "scalar(tx{host=\"missing\"})", "2024-05-10T00:03:00Z") + " | KEEP result",
            List.of(List.of(Double.NaN), List.of(Double.NaN), List.of(Double.NaN))
        );
        assertRows(query("tx / scalar(rx)") + " | KEEP result", List.of(List.of(Double.NaN)));
        assertRows(
            query(null, "scalar(vector(7))", "2024-05-10T00:03:00Z") + " | KEEP result",
            List.of(List.of(7.0), List.of(7.0), List.of(7.0))
        );
        assertRows(
            query(null, "scalar(vector(1) or label_replace(vector(1), \"host\", \"a\", \"missing\", \".*\"))", QUERY_TIME)
                + " | KEEP result",
            List.of(List.of(Double.NaN))
        );
    }

    public void testUnboundedScalarUsesTheVectorEvaluationClock() {
        createMetricsIndex(INDEX);
        index(INDEX, "cluster", "prod", "host", "a", "tx", 10.0);
        refresh(INDEX);

        String prefix = "PROMQL index=" + INDEX + " step=1m result=(";
        String suffix = ") | WHERE step == TO_DATETIME(\"2024-05-10T01:01:00Z\") | KEEP result";
        assertRows(prefix + "(tx offset 1h) / scalar(vector(1))" + suffix, List.of(List.of(10.0)));
        assertRows(prefix + "(tx offset 1h) / scalar(rx{host=\"missing\"} offset 1h)" + suffix, List.of(List.of(Double.NaN)));
    }

    /** Source-free functions use evaluation steps, including source-derived offset clocks, rather than scrape timestamps. */
    public void testClockFunctionsComposeWithRelabeling() {
        createMetricsIndex(INDEX);
        index(INDEX, "cluster", "prod", "host", "a", "tx", 10.0);
        refresh(INDEX);

        double seconds = Instant.parse(QUERY_TIME).toEpochMilli() / 1000.0;
        String relabel = "label_replace(tx, \"node\", \"$1\", \"host\", \"(.+)\")";
        assertRows(
            query(relabel + " + time()") + " | KEEP result, _timeseries",
            List.of(List.of(seconds + 10.0, "{\"cluster\":\"prod\",\"host\":\"a\",\"node\":\"a\"}"))
        );
        assertRows(query(relabel + " * pi()") + " | KEEP result", List.of(List.of(10.0 * Math.PI)));
        assertRows(query("time() + " + relabel) + " | KEEP result", List.of(List.of(seconds + 10.0)));
        assertRows(query("time() + time()") + " | KEEP result", List.of(List.of(2 * seconds)));
        assertRows(query("(time() * 2) + " + relabel) + " | KEEP result", List.of(List.of(2 * seconds + 10.0)));
        assertRows(
            "PROMQL index="
                + INDEX
                + " step=1m result=((tx offset 1h) + time())"
                + " | WHERE step == TO_DATETIME(\"2024-05-10T01:01:00Z\") | KEEP result",
            List.of(List.of(seconds + 3600.0 + 10.0))
        );
    }

    /** Explicit bounds provide a complete function clock even when no source index exists. */
    public void testSourceFreeFunctionsHaveBoundedClock() {
        double seconds = Instant.parse(QUERY_TIME).toEpochMilli() / 1000.0;
        assertRows(
            query("metrics-nonexistent-*", "time()", "2024-05-10T00:03:00Z") + " | KEEP result",
            List.of(List.of(seconds), List.of(seconds + 60.0), List.of(seconds + 120.0))
        );
        assertRows(query(null, "pi()", QUERY_TIME) + " | KEEP result", List.of(List.of(Math.PI)));
        assertRows(
            query(null, "day_of_month()", "2024-05-10T00:03:00Z") + " | KEEP result",
            List.of(List.of(10.0), List.of(10.0), List.of(10.0))
        );
        for (String expression : List.of("day_of_month() * 2", "2 * day_of_month()")) {
            assertRows(query("metrics-nonexistent-*", expression, QUERY_TIME) + " | KEEP result", List.of(List.of(20.0)));
        }
        for (String expression : List.of("1 + 2", "vector(1) + vector(2)")) {
            assertRows(query("metrics-nonexistent-*", expression, QUERY_TIME) + " | KEEP result", List.of(List.of(3.0)));
        }
        assertRows(query(null, "time() * 2", QUERY_TIME) + " | KEEP result", List.of(List.of(2 * seconds)));
    }

    /** Optional-vector date functions return one empty-label vector, so they must not broadcast as scalars. */
    public void testDateFunctionVectorMatching() {
        createMetricsIndex(INDEX);
        index(INDEX, "cluster", "prod", "host", "a", "tx", 10.0);
        refresh(INDEX);

        assertRows(query("day_of_month() + tx") + " | KEEP result", List.of());
        assertRows(query("scalar(day_of_month()) + tx") + " | KEEP result", List.of(List.of(20.0)));
        assertRows(query("tx + on() group_left day_of_month()") + " | KEEP result", List.of(List.of(20.0)));
        assertRows(query("day_of_month() or tx") + " | KEEP result", List.of(List.of(10.0), List.of(10.0)));
    }

    /** Sorting must retain typed relabeling, filters, and set membership while ordering the final instant vector. */
    public void testSortComposesWithMetadataOperations() {
        createMetricsIndex(INDEX);
        index(INDEX, "cluster", "prod", "host", "a", "tx", 10.0);
        index(INDEX, "cluster", "prod", "host", "b", "tx", 20.0, "rx", 2.0);
        refresh(INDEX);

        for (String expression : List.of("sort(label_replace(tx, \"node\", \"$1\", \"host\", \"(.+)\"))", "sort(tx > 0)")) {
            try (
                EsqlQueryResponse response = run(
                    "PROMQL index=" + INDEX + " time=\"" + QUERY_TIME + "\" result=(" + expression + ") | KEEP result"
                )
            ) {
                assertOk(response);
                assertEquals(List.of(List.of(10.0), List.of(20.0)), EsqlTestUtils.getValuesList(response));
            }
        }
        assertRows(
            "PROMQL index=" + INDEX + " time=\"" + QUERY_TIME + "\" result=(sort_desc(tx and on(host) rx)) | KEEP result",
            List.of(List.of(20.0))
        );
    }

    /** Ranking partitions select series without replacing their complete label identity. */
    public void testReductionsComposeWithRelabelingAndMatching() {
        createMetricsIndex(INDEX);
        index(INDEX, "cluster", "prod", "host", "a", "tx", 10.0, "rx", 2.0);
        index(INDEX, "cluster", "prod", "host", "b", "tx", 20.0, "rx", 3.0);
        index(INDEX, "cluster", "dev", "host", "c", "tx", 30.0, "rx", 5.0);
        refresh(INDEX);

        String relabel = "label_join(tx, \"node\", \"/\", \"cluster\", \"host\")";
        for (String expression : List.of("topk by(cluster)(1, " + relabel + ")", "topk without(host,node)(1, " + relabel + ")")) {
            assertRows(
                query(expression) + " | KEEP result, _timeseries",
                List.of(
                    List.of(20.0, "{\"cluster\":\"prod\",\"host\":\"b\",\"node\":\"prod/b\"}"),
                    List.of(30.0, "{\"cluster\":\"dev\",\"host\":\"c\",\"node\":\"dev/c\"}")
                )
            );
        }
        assertRows(
            query("topk(1, tx / on(cluster) group_left sum by(cluster)(rx))") + " | KEEP result, _timeseries",
            List.of(List.of(6.0, "{\"cluster\":\"dev\",\"host\":\"c\"}"))
        );
        assertRows(
            query("label_join(bottomk(1,tx), \"node\", \"/\", \"cluster\", \"host\")") + " | KEEP result, _timeseries",
            List.of(List.of(10.0, "{\"cluster\":\"prod\",\"host\":\"a\",\"node\":\"prod/a\"}"))
        );
        assertRows(
            query("topk(3,label_replace(sum by(host)(tx),\"dst\",\"$1\",\"host\",\"(.+)\"))") + " | KEEP result, host, dst",
            List.of(List.of(10.0, "a", "a"), List.of(20.0, "b", "b"), List.of(30.0, "c", "c"))
        );
    }

    /** A downstream promotion must not change deterministic sampling of the same logical series identity. */
    public void testReductionSamplingIgnoresPhysicalLabelRepresentation() {
        createMetricsIndex(INDEX);
        for (int i = 1; i <= 8; i++) {
            index(INDEX, "cluster", "prod", "host", "host-" + i, "tx", (double) i);
        }
        refresh(INDEX);

        try (
            EsqlQueryResponse residual = run(query("sum(limit_ratio(0.5,tx))") + " | KEEP result");
            EsqlQueryResponse promoted = run(query("sum(sum by(host)(limit_ratio(0.5,tx)))") + " | KEEP result")
        ) {
            assertOk(residual);
            assertOk(promoted);
            assertEquals(EsqlTestUtils.getValuesList(residual), EsqlTestUtils.getValuesList(promoted));
        }
        assertRows(query("sum(limit_ratio(0.5,tx) or limit_ratio(-0.5,tx))") + " | KEEP result", List.of(List.of(36.0)));
    }

    /** Constant vectors retain every requested step, while reductions with zero capacity still validate their arguments. */
    public void testReductionsWithConstantsAndDiscardedArguments() {
        assertRows(
            query(null, "topk(1,vector(7))", "2024-05-10T00:03:00Z") + " | KEEP result",
            List.of(List.of(7.0), List.of(7.0), List.of(7.0))
        );
        assertRows(query(null, "topk(0,vector(7))", QUERY_TIME) + " | KEEP result", List.of());

        createMetricsIndex(INDEX);
        index(INDEX, "cluster", "prod", "host", "a", "tx", 10.0);
        index(INDEX, "cluster", "prod", "host", "b", "tx", 20.0);
        refresh(INDEX);
        String invalid = "label_replace(tx,\"host\",\"same\",\"host\",\".*\")";
        for (String function : List.of("topk", "bottomk", "limitk", "limit_ratio")) {
            String expression = function + "(0," + invalid + ") or vector(1)";
            Exception failure = expectThrows(Exception.class, () -> run(query(expression) + " | LIMIT 1").close());
            assertThat(failure.getMessage(), containsString("input must not have duplicates"));
        }
    }

    /** Sampling preserves histogram samples; numeric ranking and sorting discard them after validating the input. */
    public void testReductionsAndSortWithHistograms() {
        createMetricsIndex(INDEX);
        index(INDEX, "cluster", "prod", "host", "a", "latency", histogram(5L));
        index(INDEX, "cluster", "prod", "host", "b", "latency", histogram(7L));
        refresh(INDEX);

        for (String expression : List.of("limitk(2,latency)", "limit_ratio(1,latency)")) {
            assertRows(query("histogram_count(" + expression + ")") + " | KEEP result", List.of(List.of(5.0), List.of(7.0)));
        }
        for (String expression : List.of("topk(1,latency)", "bottomk(1,latency)")) {
            assertRows(query(expression) + " | KEEP result", List.of());
        }
        assertRows("PROMQL index=" + INDEX + " time=\"" + QUERY_TIME + "\" result=(sort(latency)) | KEEP result", List.of());
        String invalid = "label_replace(latency,\"host\",\"same\",\"host\",\".*\")";
        for (String expression : List.of(
            "sort(" + invalid + ")",
            "sort_desc(" + invalid + ")",
            "topk(1," + invalid + ")",
            "bottomk(1," + invalid + ")"
        )) {
            for (String suffix : List.of(" | LIMIT 1", " | LIMIT 0", " | WHERE result > 1", " | WHERE false")) {
                Exception failure = expectThrows(
                    Exception.class,
                    () -> run("PROMQL index=" + INDEX + " time=\"" + QUERY_TIME + "\" result=(" + expression + ")" + suffix).close()
                );
                assertThat(failure.getMessage(), containsString("input must not have duplicates"));
            }
        }
    }

    public void testUnionAlignsLabelsPromotedOnlyByOneOperand() {
        createMetricsIndex(INDEX);
        index(INDEX, "cluster", "prod", "host", "a", "tx", 10.0);
        index(INDEX, "cluster", "prod", "host", "b", "rx", 2.0);
        refresh(INDEX);

        String relabel = "label_replace(tx, \"zone\", \"x\", \"host\", \".*\")";
        for (String expression : List.of(relabel + " or rx", "rx or " + relabel)) {
            assertRows(
                query(expression) + " | KEEP result, _timeseries",
                List.of(
                    List.of(10.0, "{\"cluster\":\"prod\",\"host\":\"a\",\"zone\":\"x\"}"),
                    List.of(2.0, "{\"cluster\":\"prod\",\"host\":\"b\"}")
                )
            );
        }
    }

    public void testMatchingLabelNamedTimeseriesDoesNotSelectMetadata() {
        createMetricsIndex(INDEX);
        index(INDEX, "cluster", "prod", "host", "a", "tx", 10.0, "rx", 2.0);
        refresh(INDEX);

        assertRows(
            query("tx / ignoring(_timeseries) rx") + " | KEEP result, _timeseries",
            List.of(List.of(5.0, "{\"cluster\":\"prod\",\"host\":\"a\"}"))
        );
        assertRows(query("tx / on(_timeseries) rx") + " | KEEP result, _timeseries", List.of(Arrays.asList(5.0, null)));
        String one = "label_replace(sum by(cluster) (rx), \"_timeseries\", \"x\", \"missing\", \".*\")";
        assertRows(
            query("tx / on(cluster) group_left(_timeseries) " + one) + " | KEEP result, _timeseries",
            List.of(List.of(5.0, "{\"_timeseries\":\"x\",\"cluster\":\"prod\",\"host\":\"a\"}"))
        );
        assertRows(
            query("sum by(host) (tx) / on() group_left(_timeseries) rx") + " | KEEP result, host, _timeseries",
            List.of(Arrays.asList(5.0, "a", null))
        );
    }

    public void testUnboundedScalarIncludesEverySetOperandClock() {
        createMetricsIndex(INDEX);
        index(INDEX, "cluster", "prod", "host", "a", "tx", 10.0);
        refresh(INDEX);

        String expression = "scalar((tx{host=\"missing\"} offset 1h) or rx{host=\"missing\"})";
        assertRows(
            "PROMQL index=" + INDEX + " step=1m result=(" + expression + ") | KEEP result",
            List.of(List.of(Double.NaN), List.of(Double.NaN))
        );
    }

    public void testMissingCopiedTimeseriesLabelRemovesThePreviousValue() {
        createMetricsIndex(INDEX);
        index(INDEX, "cluster", "prod", "host", "a", "tx", 10.0, "rx", 2.0);
        refresh(INDEX);

        String one = "sum by(cluster) (rx)";
        for (String many : List.of("tx", "label_replace(tx, \"_timeseries\", \"x\", \"\", \"\")")) {
            for (String expression : List.of(
                many + " / on(cluster) group_left(_timeseries) " + one,
                one + " / on(cluster) group_right(_timeseries) " + many
            )) {
                assertRows(
                    query(expression) + " | KEEP result, _timeseries",
                    List.of(List.of(expression.startsWith(many) ? 5.0 : 0.2, "{\"cluster\":\"prod\",\"host\":\"a\"}"))
                );
            }
        }
    }

    public void testUnionMatchesEquivalentLabelRepresentations() {
        createMetricsIndex(INDEX);
        index(INDEX, "cluster", "prod", "host", "a", "shard", 2L, "tx", 10.0, "rx", 2.0);
        refresh(INDEX);

        String relabel = "label_replace(tx, \"host\", \"$1\", \"host\", \"(.+)\")";
        assertRows(query(relabel + " or rx") + " | KEEP result", List.of(List.of(10.0)));
        assertRows(query("rx or " + relabel) + " | KEEP result", List.of(List.of(2.0)));
        String numeric = "sum by(shard) (tx)";
        String text = "label_replace(sum by(shard) (rx), \"shard\", \"2\", \"\", \"\")";
        assertRows(query(numeric + " or " + text) + " | KEEP result, shard", List.of(List.of(10.0, "2")));
        assertRows(query(text + " or " + numeric) + " | KEEP result, shard", List.of(List.of(2.0, "2")));
        for (String expression : List.of(
            "label_replace(vector(7), \"zone\", \"\", \"\", \"\") or vector(2)",
            "vector(2) or label_replace(vector(7), \"zone\", \"\", \"\", \"\")"
        )) {
            assertRows(
                query(null, expression, QUERY_TIME) + " | KEEP result",
                List.of(List.of(expression.startsWith("vector") ? 2.0 : 7.0))
            );
        }
    }

    public void testSourceFreeLabelRewriteWithoutASetOperator() {
        assertRows(
            query(null, "label_replace(vector(7), \"host\", \"a\", \"\", \"\")", QUERY_TIME) + " | KEEP result, host",
            List.of(List.of(7.0, "a"))
        );
    }

    public void testNestedAggregatesInArithmetic() {
        createMetricsIndex(INDEX);
        index(INDEX, "cluster", "prod", "host", "a", "tx", 10.0, "rx", 2.0);
        index(INDEX, "cluster", "prod", "host", "b", "tx", 20.0, "rx", 8.0);
        refresh(INDEX);

        assertRows(query("sum(max by(host) (tx)) / sum(rx)") + " | KEEP result", List.of(List.of(3.0)));
        assertRows(query("abs(sum(tx)) / ceil(sum(rx))") + " | KEEP result", List.of(List.of(3.0)));
        assertRows(
            query("sum without(host) (tx) / sum by(cluster) (rx)") + " | KEEP result, _timeseries",
            List.of(List.of(3.0, "{\"cluster\":\"prod\"}"))
        );
    }

    public void testRelabelHistogramResults() {
        createMetricsIndex(INDEX);
        index(INDEX, "cluster", "prod", "host", "a", "le", "1", "tx", 2.0);
        index(INDEX, "cluster", "prod", "host", "a", "le", "+Inf", "tx", 4.0);
        refresh(INDEX);

        String histogram = "histogram_quantile(0.5, sum by(cluster, le) (tx))";
        assertRows(
            query("label_join(" + histogram + ", \"zone\", \"/\", \"cluster\")") + " | KEEP result, cluster, zone",
            List.of(List.of(1.0, "prod", "prod"))
        );
        assertRows(
            query("label_replace(" + histogram + ", \"zone\", \"$1\", \"cluster\", \"(.+)\")") + " | KEEP result, cluster, zone",
            List.of(List.of(1.0, "prod", "prod"))
        );
        assertRows(
            query("label_join(histogram_quantile(0.5, tx), \"zone\", \"/\", \"cluster\", \"host\")") + " | KEEP result, _timeseries",
            List.of(List.of(1.0, "{\"cluster\":\"prod\",\"host\":\"a\",\"zone\":\"prod/a\"}"))
        );
    }

    public void testNativeHistogramsSurviveLabelAndSetOperations() {
        createMetricsIndex(INDEX);
        index(INDEX, "cluster", "prod", "host", "a", "latency", histogram(5L), "latency_other", histogram(9L));
        index(INDEX, "cluster", "prod", "host", "b", "latency", histogram(7L));
        refresh(INDEX);

        for (String rewrite : List.of(
            "label_replace(latency, \"zone\", \"$1\", \"host\", \"(.+)\")",
            "label_join(latency, \"zone\", \"\", \"host\")"
        )) {
            assertRows(
                query("histogram_count(" + rewrite + ")") + " | KEEP result, _timeseries",
                List.of(
                    List.of(5.0, "{\"cluster\":\"prod\",\"host\":\"a\",\"zone\":\"a\"}"),
                    List.of(7.0, "{\"cluster\":\"prod\",\"host\":\"b\",\"zone\":\"b\"}")
                )
            );
        }
        assertRows(query("histogram_count(latency or latency_other)") + " | KEEP result", List.of(List.of(5.0), List.of(7.0)));
        assertRows(query("histogram_count(latency_other or latency)") + " | KEEP result", List.of(List.of(9.0), List.of(7.0)));
        assertRows(query("histogram_count(latency and latency_other)") + " | KEEP result", List.of(List.of(5.0)));
        assertRows(query("histogram_count(latency unless latency_other)") + " | KEEP result", List.of(List.of(7.0)));
    }

    public void testNumericSamplesRejectCanonicalIdentityCollisions() {
        for (double secondValue : List.of(7.0, 9.0)) {
            String indexName = "metrics-collision-" + (int) secondValue;
            createMetricsIndex(indexName);
            index(indexName, "cluster", "prod", "host", "a", "optional", "", "tx", 7.0);
            index(indexName, "cluster", "prod", "host", "a", "tx", secondValue);
            refresh(indexName);

            assertRows("FROM " + indexName + " | STATS count = COUNT(*)", List.of(List.of(2L)));
            for (String expression : List.of("tx", "abs(tx)", "sum_over_time(tx[1m])", "label_join(tx, \"zone\", \"\", \"host\")")) {
                Exception failure = expectThrows(
                    Exception.class,
                    () -> run(query(indexName, expression, QUERY_TIME) + " | LIMIT 1").close()
                );
                assertThat(failure.getMessage(), containsString("input must not have duplicates"));
            }
            assertRows(
                query(indexName, "sum by(cluster) (tx)", QUERY_TIME) + " | KEEP result, cluster",
                List.of(List.of(7.0 + secondValue, "prod"))
            );
        }
    }

    public void testNativeHistogramsRejectCanonicalIdentityCollisions() {
        createMetricsIndex(INDEX);
        index(INDEX, "cluster", "prod", "host", "a", "optional", "", "latency", histogram(5L));
        index(INDEX, "cluster", "prod", "host", "a", "latency", histogram(7L));
        refresh(INDEX);

        // Empty and missing dimensions produce distinct source series, but the same PromQL label set.
        assertRows("FROM " + INDEX + " | STATS count = COUNT(*)", List.of(List.of(2L)));
        for (String expression : List.of(
            "histogram_count(latency)",
            "histogram_count(label_join(latency, \"zone\", \"\", \"host\"))",
            "scalar(latency)"
        )) {
            Exception failure = expectThrows(Exception.class, () -> run(query(expression) + " | LIMIT 1").close());
            assertThat(failure.getMessage(), containsString("input must not have duplicates"));
        }
        assertRows(query("histogram_count(sum by(cluster) (latency))") + " | KEEP result, cluster", List.of(List.of(12.0, "prod")));
    }

    public void testScalarIgnoresHistogramSamplesButValidatesItsInput() {
        createMetricsIndex(INDEX);
        index(INDEX, "cluster", "prod", "host", "a", "latency", histogram(5L));
        index(INDEX, "cluster", "prod", "host", "b", "latency", histogram(7L));
        refresh(INDEX);

        assertRows(query("scalar(latency{host=\"a\"})") + " | KEEP result", List.of(List.of(Double.NaN)));
        assertRows(
            query(INDEX, "scalar(latency)", "2024-05-10T00:03:00Z") + " | KEEP result",
            List.of(List.of(Double.NaN), List.of(Double.NaN), List.of(Double.NaN))
        );
        String collision = "scalar(label_replace(latency, \"host\", \"same\", \"host\", \".*\"))";
        for (String command : List.of(
            query(INDEX, collision, "2024-05-10T00:03:00Z"),
            "PROMQL index=" + INDEX + " time=\"" + QUERY_TIME + "\" result=(" + collision + ")"
        )) {
            Exception failure = expectThrows(Exception.class, () -> run(command + " | LIMIT 1").close());
            assertThat(failure.getMessage(), containsString("input must not have duplicates"));
        }
    }

    private static Map<String, Object> histogram(long count) {
        return Map.of(
            "scale",
            0,
            "positive",
            Map.of("indices", List.of(0), "counts", List.of(count)),
            "sum",
            count * 1.5,
            "min",
            1.5,
            "max",
            1.5
        );
    }

    public void testRelabelPreservesInputSeriesBeforeParentAggregation() {
        createMetricsIndex(INDEX);
        index(INDEX, "cluster", "prod", "host", "a", "tx", 3.0);
        index(INDEX, "cluster", "prod", "host", "b", "tx", 3.0);
        refresh(INDEX);

        assertRows(
            query("sum by(cluster) (label_join(tx, \"cluster\", \"\", \"cluster\"))") + " | KEEP result, cluster",
            List.of(List.of(6.0, "prod"))
        );
        assertRows(
            query("sum by(cluster) (label_replace(tx, \"cluster\", \"$1\", \"cluster\", \"(.+)\"))") + " | KEEP result, cluster",
            List.of(List.of(6.0, "prod"))
        );
    }

    public void testRelabelNamesDoNotShadowInternalColumns() {
        createMetricsIndex(INDEX);
        index(INDEX, "cluster", "prod", "host", "a", "tx", 3.0);
        refresh(INDEX);

        for (String name : List.of("labels.host", "result", "step", "_timeseries")) {
            String expected = name.equals("_timeseries")
                ? "{\"_timeseries\":\"new\",\"cluster\":\"prod\",\"host\":\"a\"}"
                : "{\"cluster\":\"prod\",\"host\":\"a\",\"" + name + "\":\"new\"}";
            assertRows(
                query("label_replace(tx, \"" + name + "\", \"new\", \"cluster\", \".*\")") + " | KEEP result, _timeseries",
                List.of(List.of(3.0, expected))
            );
        }
    }

    public void testRelabelEmptyValueIsAbsent() {
        createMetricsIndex(INDEX);
        index(INDEX, "cluster", "prod", "host", "a", "tx", 3.0);
        refresh(INDEX);

        assertRows(
            query("sum by(optional) (label_join(tx, \"optional\", \"\", \"missing\"))")
                + " | EVAL absent = optional IS NULL | KEEP result, absent",
            List.of(List.of(3.0, true))
        );
    }

    public void testFiniteLabelNamedTimeseriesIsNotMetadata() {
        createMetricsIndex(INDEX);
        index(INDEX, "cluster", "prod", "host", "a", "tx", 10.0);
        index(INDEX, "cluster", "prod", "host", "b", "tx", 20.0);
        refresh(INDEX);

        assertRows(
            query("sum by(_timeseries) (label_replace(sum by(host) (tx), \"_timeseries\", \"same\", \"host\", \".*\"))")
                + " | KEEP result, _timeseries",
            List.of(List.of(30.0, "same"))
        );
    }

    public void testValueIsAnOrdinaryLabel() {
        createMetricsIndex(INDEX);
        index(INDEX, "cluster", "prod", "host", "a", "tx", 10.0);
        index(INDEX, "cluster", "prod", "host", "b", "rx", 2.0);
        refresh(INDEX);

        assertRows(
            query(
                "label_replace(sum by(host) (tx), \"value\", \"v\", \"host\", \".*\") / ignoring(host) "
                    + "label_replace(sum by(host) (rx), \"value\", \"v\", \"host\", \".*\")"
            ) + " | KEEP result, value",
            List.of(List.of(5.0, "v"))
        );
    }

    public void testUnionKeepsMetadataDistinctFromSameNamedLabel() {
        createMetricsIndex(INDEX);
        index(INDEX, "cluster", "prod", "host", "a", "tx", 10.0);
        index(INDEX, "cluster", "prod", "host", "b", "rx", 2.0);
        refresh(INDEX);

        String relabeled = "label_replace(sum(tx), \"_timeseries\", \"x\", \"missing\", \".*\")";
        for (String expression : List.of(relabeled + " or rx", "rx or " + relabeled)) {
            assertRows(
                query(expression) + " | KEEP result, _timeseries",
                List.of(List.of(10.0, "{\"_timeseries\":\"x\"}"), List.of(2.0, "{\"cluster\":\"prod\",\"host\":\"b\"}"))
            );
            assertRows(
                query("sum by(_timeseries) (" + expression + ")") + " | KEEP result, _timeseries",
                List.of(List.of(10.0, "x"), Arrays.asList(2.0, null))
            );
        }
    }

    public void testDefaultAndIgnoringMatchFullResidualLabels() {
        createMetricsIndex(INDEX);
        index(INDEX, "cluster", "prod", "host", "a", "tx", 10.0, "rx", 2.0);
        index(INDEX, "cluster", "prod", "host", "b", "tx", 20.0);
        index(INDEX, "cluster", "prod", "host", "b", "region", "us", "rx", 8.0);
        refresh(INDEX);

        assertRows(query("tx / rx") + " | KEEP result, _timeseries", List.of(List.of(5.0, "{\"cluster\":\"prod\",\"host\":\"a\"}")));
        assertRows(
            query("tx / ignoring(region) rx") + " | KEEP result, _timeseries",
            List.of(List.of(5.0, "{\"cluster\":\"prod\",\"host\":\"a\"}"), List.of(2.5, "{\"cluster\":\"prod\",\"host\":\"b\"}"))
        );
        assertRows(query("tx / sum by(cluster) (rx)") + " | KEEP result", List.of());
        assertRows(
            query("tx / on(cluster) group_left sum by(cluster) (rx)") + " | KEEP result, _timeseries",
            List.of(List.of(1.0, "{\"cluster\":\"prod\",\"host\":\"a\"}"), List.of(2.0, "{\"cluster\":\"prod\",\"host\":\"b\"}"))
        );
    }

    public void testSetOperatorsPreserveLeftSeries() {
        createMetricsIndex(INDEX);
        index(INDEX, "cluster", "prod", "host", "a", "tx", 10.0, "rx", 2.0);
        index(INDEX, "cluster", "prod", "host", "b", "tx", 20.0);
        index(INDEX, "cluster", "prod", "host", "c", "identity", 2.0);
        index(INDEX, "cluster", "other", "host", "d", "rx", 8.0);
        refresh(INDEX);

        assertRows(query("tx and rx") + " | KEEP result", List.of(List.of(10.0)));
        assertRows(query("tx unless rx") + " | KEEP result", List.of(List.of(20.0)));
        assertRows(query("tx and on(cluster) identity") + " | KEEP result", List.of(List.of(10.0), List.of(20.0)));
        assertRows(query("tx unless on(cluster) identity") + " | KEEP result", List.of());
        assertRows(query("tx or on(cluster) identity") + " | KEEP result", List.of(List.of(10.0), List.of(20.0)));
        assertRows(query("tx or on(cluster) identity") + " | SORT result | KEEP result | LIMIT 1", List.of(List.of(10.0)));
        assertRows(query("identity or on(cluster) tx") + " | KEEP result", List.of(List.of(2.0)));
        assertRows(query("tx or identity") + " | KEEP result", List.of(List.of(10.0), List.of(20.0), List.of(2.0)));
        assertRows(query("sum(tx or on(cluster) identity)") + " | KEEP result", List.of(List.of(30.0)));
    }

    public void testNestedSetMembership() {
        createMetricsIndex(INDEX);
        index(INDEX, "cluster", "prod", "host", "a", "tx", 10.0, "rx", 2.0);
        index(INDEX, "cluster", "prod", "host", "b", "tx", 20.0);
        index(INDEX, "cluster", "prod", "host", "c", "identity", 2.0);
        refresh(INDEX);

        assertRows(query("tx and on(cluster) (identity or rx)") + " | KEEP result", List.of(List.of(10.0), List.of(20.0)));
    }

    public void testConstantVectorUsesFullLabelMatching() {
        createMetricsIndex(INDEX);
        index(INDEX, "cluster", "prod", "host", "a", "tx", 10.0);
        index(INDEX, "cluster", "prod", "host", "b", "tx", 20.0);
        refresh(INDEX);

        assertRows(query("tx + vector(1)") + " | KEEP result", List.of());
        assertRows(query("tx or vector(0)") + " | KEEP result", List.of(List.of(10.0), List.of(20.0), List.of(0.0)));
    }

    public void testUnionReconcilesRewrittenLabelType() {
        createMetricsIndex(INDEX);
        index(INDEX, "cluster", "prod", "host", "a", "shard", 1L, "tx", 10.0);
        index(INDEX, "cluster", "prod", "host", "b", "shard", 2L, "rx", 20.0);
        refresh(INDEX);

        assertRows(
            query("sum by(shard) (tx) or label_replace(sum by(shard) (rx), \"shard\", \"2\", \"shard\", \".*\")") + " | KEEP result, shard",
            List.of(List.of(10.0, "1"), List.of(20.0, "2"))
        );
        assertRows(
            query("sum by(shard) (label_replace(tx, \"shard\", \"x\", \"missing\", \".*\") or sum by(shard) (rx))")
                + " | EVAL decorated = CONCAT(shard, \"!\") | KEEP result, decorated",
            List.of(List.of(10.0, "x!"), List.of(20.0, "2!"))
        );
    }

    public void testRelabelConstantVectorInsideUnion() {
        createMetricsIndex(INDEX);
        index(INDEX, "cluster", "prod", "host", "a", "tx", 10.0);
        refresh(INDEX);

        assertRows(
            query("label_replace(vector(1), \"host\", \"a\", \"missing\", \".*\") or tx") + " | KEEP result, _timeseries",
            List.of(List.of(1.0, "{\"host\":\"a\"}"), List.of(10.0, "{\"cluster\":\"prod\",\"host\":\"a\"}"))
        );
        assertRows(
            query("label_join(vector(1), \"host\", \"-\", \"missing\", \"absent\") or tx") + " | KEEP result, _timeseries",
            List.of(List.of(1.0, "{\"host\":\"-\"}"), List.of(10.0, "{\"cluster\":\"prod\",\"host\":\"a\"}"))
        );
    }

    public void testRelabelSourceFreeConstantVectors() {
        assertRows(
            query(null, "label_replace(vector(1), \"host\", \"a\", \"missing\", \".*\") or vector(2)", QUERY_TIME) + " | KEEP result, host",
            List.of(List.of(1.0, "a"), Arrays.asList(2.0, null))
        );
        assertRows(
            query(null, "label_join(vector(1), \"host\", \"-\", \"missing\", \"absent\") or vector(2)", QUERY_TIME)
                + " | KEEP result, host",
            List.of(List.of(1.0, "-"), Arrays.asList(2.0, null))
        );
        assertRows(
            query(
                null,
                "label_join(label_replace(vector(1), \"host\", \"a\", \"missing\", \".*\"), \"zone\", \"\", \"host\") or vector(2)",
                "2024-05-10T00:03:00Z"
            ) + " | KEEP result, host, zone",
            List.of(
                List.of(1.0, "a", "a"),
                List.of(1.0, "a", "a"),
                List.of(1.0, "a", "a"),
                Arrays.asList(2.0, null, null),
                Arrays.asList(2.0, null, null),
                Arrays.asList(2.0, null, null)
            )
        );
    }

    public void testFunctionsExecuteOnConstantVectors() {
        for (var example : List.of(
            Map.entry("count(vector(7)) or vector(0)", 1.0),
            Map.entry("abs(vector(-1)) or vector(0)", 1.0),
            Map.entry("sum without(host) (label_replace(vector(7), \"host\", \"a\", \"missing\", \".*\")) or vector(0)", 7.0),
            Map.entry("histogram_quantile(0.5, vector(7)) or vector(9)", 9.0)
        )) {
            assertRows(query(null, example.getKey(), QUERY_TIME) + " | KEEP result", List.of(List.of(example.getValue())));
            assertRows(
                query(null, example.getKey(), "2024-05-10T00:03:00Z") + " | KEEP result",
                List.of(List.of(example.getValue()), List.of(example.getValue()), List.of(example.getValue()))
            );
        }
    }

    public void testConstantAggregationIsIndependentOfSourceRows() {
        createMetricsIndex(INDEX);
        index(INDEX, "cluster", "prod", "host", "a", "tx", 10.0);
        index(INDEX, "cluster", "prod", "host", "b", "tx", 20.0);
        refresh(INDEX);

        assertRows(
            query(INDEX, "count(vector(7)) or sum(tx)", "2024-05-10T00:03:00Z") + " | KEEP result",
            List.of(List.of(1.0), List.of(1.0), List.of(1.0))
        );
    }

    public void testPromotedLabelRetainsSelectorFilter() {
        createMetricsIndex(INDEX);
        index(INDEX, "cluster", "prod", "host", "a", "tx", 10.0);
        index(INDEX, "cluster", "prod", "host", "b", "tx", 20.0);
        refresh(INDEX);

        assertRows(query("sum by(host) (tx{host=\"a\"})") + " | KEEP result, host", List.of(List.of(10.0, "a")));
        assertRows(
            query("sum by(host) (label_replace(tx{host=\"a\"}, \"host\", \"renamed\", \"host\", \".*\"))") + " | KEEP result, host",
            List.of(List.of(10.0, "renamed"))
        );
    }

    public void testSetMatchingStoredMetricNameLabel() {
        createMetricsIndex(INDEX);
        index(INDEX, "cluster", "prod", "host", "a", LabelMatcher.NAME, "left", "tx", 10.0);
        index(INDEX, "cluster", "prod", "host", "a", LabelMatcher.NAME, "right", "rx", 2.0);
        refresh(INDEX);

        assertRows(query("tx and rx") + " | KEEP result", List.of(List.of(10.0)));
        assertRows(query("tx and on(" + LabelMatcher.NAME + ") rx") + " | KEEP result", List.of());
        assertRows(query("tx unless on(" + LabelMatcher.NAME + ") rx") + " | KEEP result", List.of(List.of(10.0)));
        assertRows(query("tx or on(" + LabelMatcher.NAME + ") rx") + " | KEEP result", List.of(List.of(10.0), List.of(2.0)));
    }

    public void testFunctionsDropMetricNameAfterEvaluatingEachSeries() {
        createMetricsIndex(INDEX);
        index(INDEX, "cluster", "prod", "host", "a", LabelMatcher.NAME, "metric", "tx", 10.0);
        refresh(INDEX);

        for (String expression : List.of("abs(tx)", "ceil(tx)", "sum_over_time(tx[1m])")) {
            assertRows(query(expression) + " | KEEP result, _timeseries", List.of(List.of(10.0, "{\"cluster\":\"prod\",\"host\":\"a\"}")));
        }
        assertRows(
            query("last_over_time(tx[1m])") + " | KEEP result, _timeseries",
            List.of(List.of(10.0, "{\"" + LabelMatcher.NAME + "\":\"metric\",\"cluster\":\"prod\",\"host\":\"a\"}"))
        );
        assertRows(
            query("abs(sum by(" + LabelMatcher.NAME + ") (tx))") + " | KEEP result, " + LabelMatcher.NAME,
            List.of(Arrays.asList(10.0, null))
        );

        index(INDEX, "cluster", "prod", "host", "a", LabelMatcher.NAME, "other", "tx", 20.0);
        refresh(INDEX);
        for (String expression : List.of("abs(tx)", "sum_over_time(tx[1m])")) {
            Exception failure = expectThrows(Exception.class, () -> run(query(expression) + " | LIMIT 1").close());
            assertThat(failure.getMessage(), containsString("input must not have duplicates"));
        }
    }

    public void testKeywordNumericSourceTokensMatchIndexedLabelIdentity() {
        createMetricsIndex(INDEX, Settings.builder().put("index.mapping.source.mode", "stored").build());
        client().prepareIndex(INDEX).setSource("""
            {"@timestamp":"2024-05-10T00:01:00Z","cluster":"prod","host":1e3,"tx":7.0}
            """, XContentType.JSON).get();
        client().prepareIndex(INDEX).setSource("""
            {"@timestamp":"2024-05-10T00:01:00Z","cluster":"prod","host":1000.0,"tx":9.0}
            """, XContentType.JSON).get();
        refresh(INDEX);

        // Keyword indexing retains numeric token spelling; residual metadata must expose exactly the same labels.
        assertRows(query("sum by(host) (tx)") + " | KEEP result, host", List.of(List.of(7.0, "1e3"), List.of(9.0, "1000.0")));
        assertRows(
            query("tx") + " | KEEP result, _timeseries",
            List.of(List.of(7.0, "{\"cluster\":\"prod\",\"host\":\"1e3\"}"), List.of(9.0, "{\"cluster\":\"prod\",\"host\":\"1000.0\"}"))
        );
        assertRows(
            query("sum without(cluster) (tx) / sum by(host) (tx)") + " | KEEP result, _timeseries",
            List.of(List.of(1.0, "{\"host\":\"1e3\"}"), List.of(1.0, "{\"host\":\"1000.0\"}"))
        );
    }

    public void testResidualLabelsUseIndexedDimensionsWhenStoredSourceOmitsThem() {
        assertAcked(
            indicesAdmin().prepareCreate(INDEX)
                .setSettings(
                    Settings.builder()
                        .put("index.mode", "time_series")
                        .put("index.mapping.source.mode", "stored")
                        .putList("index.routing_path", "cluster")
                        .put("index.time_series.start_time", "2024-05-10T00:00:00Z")
                        .put("index.time_series.end_time", "2024-05-10T01:00:00Z")
                        .put("index.number_of_shards", 2)
                        .put("index.number_of_replicas", 0)
                        .put("index.routing.allocation.total_shards_per_node", 1)
                )
                .setMapping("""
                    {
                      "_source": {"excludes": ["host"]},
                      "properties": {
                        "@timestamp": {"type": "date"},
                        "cluster": {"type": "keyword", "time_series_dimension": true},
                        "host": {"type": "keyword", "time_series_dimension": true},
                        "tx": {"type": "double", "time_series_metric": "gauge"}
                      }
                    }
                    """)
        );
        ensureGreen(INDEX);
        var indexed = client().prepareIndex(INDEX).setSource("""
            {"@timestamp":"2024-05-10T00:01:00Z","cluster":"prod","host":1e3,"tx":7.0}
            """, XContentType.JSON).get();
        refresh(INDEX);
        assertFalse(client().prepareGet(INDEX, indexed.getId()).get().getSourceAsMap().containsKey("host"));

        assertRows(query("sum by(host) (tx)") + " | KEEP result, host", List.of(List.of(7.0, "1e3")));
        assertRows(query("tx") + " | KEEP result, _timeseries", List.of(List.of(7.0, "{\"cluster\":\"prod\",\"host\":\"1e3\"}")));
        assertRows(
            query("sum without(cluster) (tx) / sum by(host) (tx)") + " | KEEP result, _timeseries",
            List.of(List.of(1.0, "{\"host\":\"1e3\"}"))
        );
    }

    public void testNumericAndIpResidualMatchesPromotedIdentity() {
        createMetricsIndex(INDEX, Settings.builder().put("index.mapping.source.mode", "stored").build());
        index(INDEX, "cluster", "prod", "host", "a", "shard", "01", "ip", "2001:0db8:0:0:0:0:0:1", "tx", 7.0);
        refresh(INDEX);

        assertRows(
            query("tx") + " | KEEP result, _timeseries",
            List.of(List.of(7.0, "{\"cluster\":\"prod\",\"host\":\"a\",\"ip\":\"2001:db8::1\",\"shard\":\"1\"}"))
        );
        assertRows(
            query("sum without(host) (tx) / sum by(cluster, shard, ip) (tx)") + " | KEEP result, _timeseries",
            List.of(List.of(1.0, "{\"cluster\":\"prod\",\"ip\":\"2001:db8::1\",\"shard\":\"1\"}"))
        );
        assertRows(
            query("tx / on(shard, ip) group_left sum by(shard, ip) (tx)") + " | KEEP result, _timeseries",
            List.of(List.of(1.0, "{\"cluster\":\"prod\",\"host\":\"a\",\"ip\":\"2001:db8::1\",\"shard\":\"1\"}"))
        );
        assertRows(query("sum by(shard) (tx)") + " | EVAL next_shard = shard + 1 | KEEP result, next_shard", List.of(List.of(7.0, 2L)));
        assertRows(
            query("sum by(shard) (label_replace(tx, \"shard\", \"2\", \"shard\", \".*\"))") + " | KEEP result, shard",
            List.of(List.of(7.0, "2"))
        );
    }

    public void testEmptyAndAbsentLabelsHaveSameIdentity() {
        createMetricsIndex(INDEX);
        index(INDEX, "cluster", "prod", "host", "a", "optional", "", "tx", 10.0);
        index(INDEX, "cluster", "prod", "host", "b", "tx", 20.0);
        refresh(INDEX);

        assertRows(query("sum without(host) (tx)") + " | KEEP result, _timeseries", List.of(List.of(30.0, "{\"cluster\":\"prod\"}")));
        assertRows(query("sum by(optional) (tx)") + " | KEEP result, optional", List.of(Arrays.asList(30.0, null)));
        assertRows(
            query("tx{host=\"a\"} / ignoring(host) tx{host=\"b\"}") + " | KEEP result, _timeseries",
            List.of(List.of(0.5, "{\"cluster\":\"prod\"}"))
        );
    }

    public void testDuplicateRelabelFailsEvenWithLimit() {
        // Each input is on a different node, so a local check cannot establish output uniqueness.
        String[] nodes = internalCluster().getNodeNames();
        createMetricsIndex("metrics-left", singleShardOn(nodes[0]));
        createMetricsIndex("metrics-right", singleShardOn(nodes[1]));
        index("metrics-left", "cluster", "prod", "host", "a", "tx", 10.0);
        index("metrics-right", "cluster", "prod", "host", "b", "tx", 20.0);
        refresh("metrics-left", "metrics-right");

        String query = query("metrics-*", "label_replace(tx, \"host\", \"same\", \"host\", \".*\")", QUERY_TIME);
        for (String suffix : List.of("", " | LIMIT 1")) {
            Exception failure = expectThrows(Exception.class, () -> run(query + suffix).close());
            assertThat(failure.getMessage(), containsString("input must not have duplicates"));
        }
    }

    public void testHistogramWithoutBucketsStillValidatesItsArgument() {
        createMetricsIndex(INDEX);
        index(INDEX, "cluster", "prod", "host", "a", "tx", 10.0);
        index(INDEX, "cluster", "prod", "host", "b", "tx", 20.0);
        refresh(INDEX);

        String invalid = "label_replace(sum by(host) (tx), \"host\", \"same\", \"host\", \".*\")";
        String discarded = "histogram_quantile(0.5, " + invalid + ")";
        for (String expression : List.of(
            discarded + " or vector(2)",
            "scalar(" + discarded + ")",
            "vector(scalar(" + discarded + ")) or vector(2)"
        )) {
            for (String end : List.of(QUERY_TIME, "2024-05-10T00:03:00Z")) {
                Exception failure = expectThrows(Exception.class, () -> run(query(INDEX, expression, end) + " | LIMIT 1").close());
                assertThat(failure.getMessage(), containsString("input must not have duplicates"));
            }
        }
    }

    public void testEmptySetOperandDoesNotBypassOtherOperandValidation() {
        createMetricsIndex(INDEX);
        index(INDEX, "cluster", "prod", "host", "a", "tx", 10.0);
        index(INDEX, "cluster", "prod", "host", "b", "tx", 20.0);
        refresh(INDEX);

        String invalid = "label_replace(sum by(host) (tx), \"host\", \"same\", \"host\", \".*\")";
        for (String empty : List.of("rx", "histogram_quantile(0.5, vector(7))", "(vector(1) > 2)")) {
            for (String operation : List.of("and", "unless")) {
                for (String expression : List.of(invalid + " " + operation + " " + empty, empty + " " + operation + " " + invalid)) {
                    Exception failure = expectThrows(
                        Exception.class,
                        () -> run(query("(" + expression + ") or vector(2)") + " | LIMIT 1").close()
                    );
                    assertThat(failure.getMessage(), containsString("input must not have duplicates"));
                }
            }
        }
    }

    public void testEmptyArithmeticOperandDoesNotBypassOtherOperandValidation() {
        createMetricsIndex(INDEX);
        index(INDEX, "cluster", "prod", "host", "a", "tx", 10.0);
        index(INDEX, "cluster", "prod", "host", "b", "tx", 20.0);
        refresh(INDEX);

        String invalid = "label_replace(tx, \"host\", \"same\", \"host\", \".*\")";
        for (String expression : List.of("(vector(1) > 2) + on() group_left " + invalid, "(vector(1) > 2) + scalar(" + invalid + ")")) {
            Exception failure = expectThrows(Exception.class, () -> run(query(expression) + " | LIMIT 1").close());
            assertThat(failure.getMessage(), containsString("input must not have duplicates"));
        }
    }

    public void testOneToOneJoinRejectsMatchedDuplicatesWithLimit() {
        String[] nodes = internalCluster().getNodeNames();
        createMetricsIndex("metrics-left", singleShardOn(nodes[0]));
        createMetricsIndex("metrics-right", singleShardOn(nodes[1]));
        index("metrics-left", "cluster", "prod", "host", "a", "tx", 10.0, "identity", 2.0);
        index("metrics-right", "cluster", "prod", "host", "b", "tx", 20.0);
        refresh("metrics-left", "metrics-right");

        // Both probes match the same build row. Even after one successful result, the other node's
        // probe must be consumed so the one-to-one cardinality violation is reported.
        for (String expression : List.of(
            "tx / on(cluster) identity",
            "tx > on(cluster) identity",
            "sum by(cluster, host) (tx) / on(cluster) sum by(cluster) (identity)"
        )) {
            Exception failure = expectThrows(Exception.class, () -> run(query("metrics-*", expression, QUERY_TIME) + " | LIMIT 1").close());
            assertThat(failure.getMessage(), containsString("input must not have duplicates"));
        }
    }

    public void testOneToOneJoinAllowsUnmatchedDuplicateProbeKeys() {
        String[] nodes = internalCluster().getNodeNames();
        createMetricsIndex("metrics-left", singleShardOn(nodes[0]));
        createMetricsIndex("metrics-right", singleShardOn(nodes[1]));
        index("metrics-left", "cluster", "prod", "host", "a", "tx", 10.0, "identity", 2.0);
        index("metrics-left", "cluster", "missing", "host", "b", "tx", 20.0);
        index("metrics-right", "cluster", "missing", "host", "c", "tx", 30.0);
        refresh("metrics-left", "metrics-right");

        for (String expression : List.of(
            "tx / on(cluster) identity",
            "sum by(cluster, host) (tx) / on(cluster) sum by(cluster) (identity)"
        )) {
            assertRows(query("metrics-*", expression, QUERY_TIME) + " | KEEP result | LIMIT 1", List.of(List.of(5.0)));
        }
        assertRows(query("metrics-*", "tx > on(cluster) identity", QUERY_TIME) + " | KEEP result | LIMIT 1", List.of(List.of(10.0)));
    }

    public void testRelabelAllowsSameIdentityAtDifferentSteps() {
        createMetricsIndex(INDEX);
        index(INDEX, "cluster", "prod", "host", "a", "tx", 7.0);
        client().prepareIndex(INDEX).setSource("@timestamp", "2024-05-10T00:02:00Z", "cluster", "prod", "host", "a", "tx", 9.0).get();
        refresh(INDEX);

        assertRows(
            query(INDEX, "label_replace(tx, \"host\", \"same\", \"host\", \".*\")", "2024-05-10T00:02:00Z") + " | KEEP result, _timeseries",
            List.of(List.of(7.0, "{\"cluster\":\"prod\",\"host\":\"same\"}"), List.of(9.0, "{\"cluster\":\"prod\",\"host\":\"same\"}"))
        );
    }

    private void createMetricsIndex(String name) {
        createMetricsIndex(name, Settings.EMPTY);
    }

    private void createMetricsIndex(String name, Settings overrides) {
        assertAcked(
            indicesAdmin().prepareCreate(name)
                .setSettings(
                    Settings.builder()
                        .put("index.mode", "time_series")
                        .putList("index.routing_path", "cluster", "host")
                        .put("index.time_series.start_time", "2024-05-10T00:00:00Z")
                        .put("index.time_series.end_time", "2024-05-10T01:00:00Z")
                        .put("index.number_of_shards", 2)
                        .put("index.number_of_replicas", 0)
                        .put("index.routing.allocation.total_shards_per_node", 1)
                        .put(overrides)
                )
                .setMapping(
                    "@timestamp",
                    "type=date",
                    "cluster",
                    "type=keyword,time_series_dimension=true",
                    "host",
                    "type=keyword,time_series_dimension=true",
                    "region",
                    "type=keyword,time_series_dimension=true",
                    "optional",
                    "type=keyword,time_series_dimension=true",
                    "le",
                    "type=keyword,time_series_dimension=true",
                    LabelMatcher.NAME,
                    "type=keyword,time_series_dimension=true",
                    "shard",
                    "type=long,time_series_dimension=true",
                    "ip",
                    "type=ip,time_series_dimension=true",
                    "tx",
                    "type=double,time_series_metric=gauge",
                    "rx",
                    "type=double,time_series_metric=gauge",
                    "identity",
                    "type=double,time_series_metric=gauge",
                    "latency",
                    "type=exponential_histogram,time_series_metric=histogram",
                    "latency_other",
                    "type=exponential_histogram,time_series_metric=histogram"
                )
        );
        ensureGreen(name);
    }

    private static Settings singleShardOn(String node) {
        return Settings.builder().put("index.number_of_shards", 1).put("index.routing.allocation.require._name", node).build();
    }

    private void index(String index, Object... fields) {
        Object[] source = new Object[fields.length + 2];
        source[0] = "@timestamp";
        // Range queries read from their start time, so keep samples on the evaluated step.
        source[1] = QUERY_TIME;
        System.arraycopy(fields, 0, source, 2, fields.length);
        client().prepareIndex(index).setSource(source).get();
    }

    private static String query(String expression) {
        return query(INDEX, expression, QUERY_TIME);
    }

    private static String query(String index, String expression, String end) {
        return "PROMQL "
            + (index == null ? "" : "index=" + index + " ")
            + "start=\""
            + QUERY_TIME
            + "\" end=\""
            + end
            + "\" step=1m result=("
            + expression
            + ")";
    }

    private void assertRows(String query, List<List<Object>> expected) {
        try (EsqlQueryResponse response = run(query)) {
            assertOk(response);
            assertThat(EsqlTestUtils.getValuesList(response), containsInAnyOrder(expected.toArray()));
        }
    }
}
