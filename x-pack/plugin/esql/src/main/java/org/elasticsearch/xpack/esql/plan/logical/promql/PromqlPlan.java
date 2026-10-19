/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesGet;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesHash;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesUnset;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionDefinition;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionRegistry;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.TemporaryNameGenerator;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContext.IntermediateResult;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContext.supportTimeSeriesMetadataType;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationSchema.finite;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationSchema.union;

/**
 * PromQL logical nodes and their shared lowering into ES|QL plans.
 */
public interface PromqlPlan {
    /**
     * Returns any grouping attributes, for example those added via {@code by(...)},
     * or {@link FieldAttribute#timeSeriesAttribute(Source)} (group by all).
     * <p>
     * Note: The value and step column are added by {@link PromqlCommand#output()}
     * and should not be added by implementations of this interface.
     */
    List<Attribute> output();

    /**
     * Known types of logical labels, including labels carried inside metadata. A missing entry means the type is
     * unknown, not that the label is absent. These facts never add output columns or bind physical attributes.
     * Nodes that preserve labels may propagate facts; nodes that remove labels must remove their facts as well.
     */
    default Map<String, DataType> labelTypes() {
        var types = new HashMap<String, DataType>();
        for (Attribute attribute : output()) {
            if (attribute.resolved() && PromqlLabels.isTsMetadata(attribute) == false) {
                types.put(PromqlLabels.labelName(attribute), attribute.dataType());
            }
        }
        return Map.copyOf(types);
    }

    static Map<String, DataType> labelTypes(LogicalPlan plan) {
        return plan instanceof PromqlPlan promql ? promql.labelTypes() : Map.of();
    }

    /**
     * The PromQL return type of this plan node.
     */
    PromqlDataType returnType();

    /**
     * Translates this node into an ES|QL plan. {@code translation} carries what the enclosing node
     * {@link TranslationContext#required() requires} of this node's labels and the shared services: recursion into the child
     * under a requirement, and the aggregation and {@link TranslationContext#eval eval} helpers
     * every node composes its result from. Each node owns its translation the way it owns its {@link #output()}.
     */
    TranslationContext.IntermediateResult translate(TranslationContext translation);

    /**
     * Materialize labels required by the parent after this node has translated. Requirements describe access,
     * not projection: all other labels must still distinguish series. Each promoted label leaves the residual metadata.
     */
    default IntermediateResult translateWithRequirements(TranslationContext context) {
        IntermediateResult input = translate(context);
        if (supportTimeSeriesMetadataType(context) == false) {
            return input;
        }
        var missing = input.schema().labelsInMetadata(context.required().labels());
        if (missing.isEmpty()) {
            return input;
        }
        Source source = input.plan().source();
        Attribute metadata = input.metadata();
        var aliases = new ArrayList<Alias>();
        var bindings = new HashMap<>(input.labelBindings());
        var names = new ArrayList<Expression>();
        for (String name : missing) {
            Literal label = Literal.keyword(source, name);
            Alias alias = new Alias(
                source,
                TemporaryNameGenerator.locallyUniqueTemporaryName("label"),
                new TimeSeriesGet(source, metadata, label)
            );
            aliases.add(alias);
            bindings.put(name, alias.toAttribute());
            names.add(label);
        }
        aliases.add(new Alias(source, MetadataAttribute.TIMESERIES, new TimeSeriesUnset(source, metadata, names)));
        return input.with(new Eval(source, input.plan(), aliases), union(input.schema(), finite(missing)), input.value(), bindings);
    }

    /** Build the full series identity used by aggregation and duplicate detection. */
    static Expression seriesKey(IntermediateResult input) {
        Source source = input.plan().source();
        var labels = new ArrayList<Expression>();
        for (String name : new TreeSet<>(input.schema().labels())) {
            labels.add(Literal.keyword(source, name));
            labels.add(input.labelColumn(name));
        }
        Attribute metadata = input.metadata();
        return new TimeSeriesHash(source, metadata == null ? new Literal(source, null, DataType.TIMESERIES_DATA_TYPE) : metadata, labels);
    }

    /** Reject distinct input series that a label rewrite maps to the same output identity. */
    static IntermediateResult checkUnique(Source source, IntermediateResult input) {
        // Missing samples are absent from the vector and must not create duplicate series.
        LogicalPlan present = new Filter(source, input.plan(), new IsNotNull(source, input.value()));
        return input.with(new CheckSeriesUniqueness(source, present, input.step(), seriesKey(input)), input.schema(), input.value());
    }

    /**
     * Whether this node is transparent to relabel ({@code label_replace}/{@code label_join}) placement: it does NOT form a
     * series-identity grouping/partition/matching boundary, so a relabel appearing below it is consumed by this node's own
     * enclosing consumer rather than by this node.
     * <p>
     * Return {@code false} only for nodes that bind the enclosing series identity - across-series aggregation
     * ({@code by}/{@code without}), across-series reduction ({@code topk}/{@code bottomk}), and vector binary operators.
     * Per-series and value/type-shaping nodes (selectors, {@code rate}-style within-series aggregates, scalar/vector
     * conversions, {@code histogram_quantile}) are transparent for this purpose even when they reshape labels, because a
     * relabel below them still feeds the same enclosing aggregation. This drives the supported-placement check for the
     * label functions; see {@code PromqlCommand#verifyMetadataManipulationPlacement}.
     */
    default boolean isIdentityTransparent() {
        return true;
    }

    /**
     * Utility methods to check the return type of a PromqlPlan
     *
     * @param plan the logical plan to check
     * @return true if the plan returns a range vector
     * @throws IllegalArgumentException if the plan is not a PromqlPlan
     */
    static boolean returnsRangeVector(LogicalPlan plan) {
        return getType(plan) == PromqlDataType.RANGE_VECTOR;
    }

    /**
     * Utility methods to check the return type of a PromqlPlan
     *
     * @param plan the logical plan to check
     * @return true if the plan returns an instant vector
     * @throws IllegalArgumentException if the plan is not a PromqlPlan
     */
    static boolean returnsInstantVector(LogicalPlan plan) {
        return getType(plan) == PromqlDataType.INSTANT_VECTOR;
    }

    /**
     * Utility methods to check the return type of a PromqlPlan
     *
     * @param plan the logical plan to check
     * @return true if the plan returns a scalar
     * @throws IllegalArgumentException if the plan is not a PromqlPlan
     */
    static boolean returnsScalar(LogicalPlan plan) {
        return plan.resolved() && getType(plan) == PromqlDataType.SCALAR;
    }

    /**
     * Returns the PromQL data type for the given plan, or {@code null} if it's not a PromQL plan.
     * Handles {@link UnresolvedPromqlFunction} by looking up the function's output type from the registry.
     */
    @Nullable
    static PromqlDataType getType(@Nullable LogicalPlan plan) {
        if (plan instanceof UnresolvedPromqlFunction unresolved) {
            PromqlFunctionDefinition def = PromqlFunctionRegistry.INSTANCE.functionMetadata(unresolved.functionName());
            return def != null ? def.functionType().outputType : null;
        }
        if (plan instanceof PromqlPlan promqlPlan) {
            return promqlPlan.returnType();
        }
        return null;
    }
}
