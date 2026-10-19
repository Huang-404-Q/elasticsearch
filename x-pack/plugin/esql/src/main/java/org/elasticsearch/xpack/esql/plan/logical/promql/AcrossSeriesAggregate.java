/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.xpack.esql.core.capabilities.Resolvables;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesUnset;
import org.elasticsearch.xpack.esql.expression.promql.function.FunctionType;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionDefinition;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionRegistry.PromqlContext;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContext.IntermediateResult;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.LabelMatcher;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.xpack.esql.plan.logical.promql.AcrossSeriesAggregate.Grouping.WITHOUT;
import static org.elasticsearch.xpack.esql.plan.logical.promql.PromqlLabels.hasTsMetadata;
import static org.elasticsearch.xpack.esql.plan.logical.promql.PromqlLabels.labelName;
import static org.elasticsearch.xpack.esql.plan.logical.promql.PromqlLabels.labelNames;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContext.supportTimeSeriesMetadataType;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationSchema.finite;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationSchema.open;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationSchema.project;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationSchema.subtract;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationSchema.union;

/**
 * Represents a PromQL aggregate function call that operates across multiple time series.
 * <p>
 * These functions aggregate elements from multiple time series into a single result vector,
 * optionally grouping by specific labels. This corresponds to PromQL syntax:
 * <pre>
 * function_name(instant_vector) [without|by (label_list)]
 * </pre>
 *
 * Examples:
 * <pre>
 * sum(http_requests_total)
 * sum(rate(http_requests_total[5m]))
 * avg(cpu_usage) by (host, env)
 * max(response_time) without (instance)
 * </pre>
 *
 * These functions reduce the number of time series by aggregating values across series
 * that share the same grouping labels (or all series if no grouping is specified).
 */
public final class AcrossSeriesAggregate extends PromqlFunctionCall {

    public enum Grouping {
        BY,
        WITHOUT,
        NONE
    }

    private final Grouping grouping;
    private final List<Attribute> groupings;
    private final Attribute timeseriesAttribute;

    public AcrossSeriesAggregate(
        Source source,
        LogicalPlan child,
        PromqlFunctionDefinition definition,
        List<Expression> parameters,
        Grouping grouping,
        List<Attribute> groupings
    ) {
        super(source, child, definition, parameters);
        this.grouping = grouping;
        this.groupings = groupings;
        this.timeseriesAttribute = FieldAttribute.timeSeriesAttribute(source);
    }

    public Grouping grouping() {
        return grouping;
    }

    public List<Attribute> groupings() {
        return groupings;
    }

    @Override
    public boolean expressionsResolved() {
        return Resolvables.resolved(groupings) && super.expressionsResolved();
    }

    @Override
    protected NodeInfo<PromqlFunctionCall> info() {
        return NodeInfo.create(this, AcrossSeriesAggregate::new, child(), definition(), parameters(), grouping(), groupings());
    }

    @Override
    public AcrossSeriesAggregate replaceChild(LogicalPlan newChild) {
        return new AcrossSeriesAggregate(source(), newChild, definition(), parameters(), grouping(), groupings());
    }

    // @Override
    // public String telemetryLabel() {
    // return "PROMQL_ACROSS_SERIES_AGGREGATION";
    // }

    @Override
    public boolean equals(Object o) {
        if (super.equals(o)) {
            AcrossSeriesAggregate that = (AcrossSeriesAggregate) o;
            return grouping == that.grouping && Objects.equals(groupings, that.groupings);
        }
        return false;
    }

    /**
     * {@code WITHOUT} over a child with metadata identity (a selector, a function over one) uses a dynamic
     * {@code _timeseries} output, because the concrete retained labels are not known until lowering time.
     * {@code WITHOUT} over a child that names every label it exposes (a {@code BY}/{@code NONE} aggregate, a binary
     * operator between two of them) instead exposes those labels minus the excluded ones - the {@code WITHOUT} is a plain
     * re-grouping over known columns, so it must NOT claim a {@code _timeseries} the plan never produces. {@code BY} and
     * {@code NONE} export concrete labels or nothing.
     */
    @Override
    public List<Attribute> output() {
        // Output `_timeseries` if grouping is not constant, e.g. `without(...)`
        if (grouping == Grouping.WITHOUT) {
            List<Attribute> childOutput = child().output();
            if (hasTsMetadata(childOutput) == false) {
                Set<String> excluded = new HashSet<>();
                for (Attribute label : groupings) {
                    excluded.add(labelName(label));
                }
                return childOutput.stream().filter(a -> excluded.contains(labelName(a)) == false).toList();
            }
            return List.of(timeseriesAttribute);
        }
        // A label that resolved to a metric field is not a real label, so translation drops it from the
        // aggregate; exclude it from the output too, otherwise the command projection references a column the
        // plan never produces. Absent labels are already excluded by the resolved() check.
        return groupings.stream()
            .filter(a -> a.resolved() && a.dataType() != DataType.NULL)
            .filter(a -> (a instanceof FieldAttribute fa && fa.isMetric()) == false)
            .toList();
    }

    @Override
    public Map<String, DataType> labelTypes() {
        if (grouping != WITHOUT) {
            return super.labelTypes();
        }
        var types = new HashMap<>(PromqlPlan.labelTypes(child()));
        groupings.forEach(attribute -> types.remove(labelName(attribute)));
        types.remove(LabelMatcher.NAME);
        return Map.copyOf(types);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), grouping, groupings);
    }

    @Override
    public FunctionType functionType() {
        return FunctionType.ACROSS_SERIES_AGGREGATION;
    }

    @Override
    public boolean isIdentityTransparent() {
        // Aggregates across series into a grouped result: a relabel below it must be part of this grouping's identity.
        return false;
    }

    /**
     * Translates {@code AcrossSeriesAggregate} to an ESQL {@code Aggregate}. The schema transposed below the
     * aggregate names every column the subtree must expose, so the child translates once and the aggregate's own
     * columns are read off the returned schema. Only {@code AcrossSeriesAggregate} creates plan-level aggregation
     * nodes; within-series aggregates and function calls lower to expressions.
     */
    @Override
    public IntermediateResult translate(TranslationContext context) {
        List<String> keys = labelNames(groupings());
        TranslationSchema childRequired = switch (grouping()) {
            case BY -> finite(keys);
            // without () keeps the child's label set; without (K) declares its own and widens every pending one by K
            case WITHOUT -> keys.isEmpty() ? context.required() : union(subtract(context.required(), keys), open(keys));
            case NONE -> TranslationSchema.EMPTY;
        };
        TranslationContext childTranslation = context.withRequired(childRequired);
        IntermediateResult ir = childTranslation.translate(child());
        if (skipFunctionTranslation(context, ir)) {
            return ir;
        }
        if (supportTimeSeriesMetadataType(context) && ir.schema().equals(TranslationSchema.EMPTY) && ir.value().foldable()) {
            // A constant vector has one sample per step, regardless of the number of source documents.
            ir = context.inlineConstantSteps(ir);
            if (ir.kind().afterInitialAggregation == false) {
                ir = context.collapse(ir, ir.schema(), ir.value());
            }
        }
        if (supportTimeSeriesMetadataType(context) && grouping() == WITHOUT) {
            var excluded = new LinkedHashSet<>(keys);
            excluded.add(LabelMatcher.NAME);
            var metadataLabels = ir.schema().labelsInMetadata(excluded);
            LogicalPlan plan = ir.plan();
            if (metadataLabels.isEmpty() == false) {
                var names = metadataLabels.stream().map(name -> (Expression) Literal.keyword(source(), name)).toList();
                var metadata = new TimeSeriesUnset(source(), ir.metadata(), names);
                plan = new Eval(source(), plan, List.of(new Alias(source(), MetadataAttribute.TIMESERIES, metadata)));
            }
            var remaining = new LinkedHashSet<>(ir.schema().labels());
            remaining.removeAll(excluded);
            ir = ir.with(plan, project(ir.schema(), remaining), ir.value());
        }
        TranslationSchema schema = switch (grouping()) {
            case BY -> finite(labelNames(output()));
            case WITHOUT -> context.regroupWithout(ir.schema(), keys);
            case NONE -> TranslationSchema.EMPTY;
        };

        var promqlCtx = new PromqlContext(context.time(), AggregateFunction.NO_WINDOW, ir.step(), context.configuration());
        Expression function = buildEsqlFunction(ir.value(), promqlCtx);
        // A raw operand collapses once, with the operator's function fused into the per-series aggregate; a table regroups.
        IntermediateResult result = ir.kind().afterInitialAggregation
            ? context.regroup(ir, schema, grouping() == WITHOUT, function)
            : context.collapse(ir, schema, function);
        // A count is at least 1 for an element. A group none of whose series has a value - every series of an operator
        // left without a partner, say - is no element in Prometheus, not a count of 0.
        if (function instanceof Count) {
            result = context.eval(result, TranslationContext.zeroCountAsNull(result.valueColumn()));
        }
        return result;
    }
}
