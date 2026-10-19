/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.capabilities.Resolvables;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.UnionTypeEsField;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToCounter;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToGauge;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesUnset;
import org.elasticsearch.xpack.esql.expression.promql.function.FunctionType;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionDefinition;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionRegistry;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionRegistry.PromqlContext;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContext.IntermediateResult;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.LabelMatcher;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.RangeSelector;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.Selector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContext.supportTimeSeriesMetadataType;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationSchema.open;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationSchema.union;

/**
 * Represents a PromQL function call in the logical plan.
 *
 * This is a surrogate logical plan that encapsulates a PromQL function invocation
 * and delegates to the PromqlFunctionRegistry for validation and ESQL function construction.
 */
public abstract sealed class PromqlFunctionCall extends UnaryPlan implements PromqlPlan permits AcrossSeriesAggregate,
    AcrossSeriesReduction, HistogramFunctionCall, MetadataManipulationFunction, ScalarConversionFunction, SortFunction,
    WithinSeriesAggregate, ValueTransformationFunction, VectorConversionFunction {
    // implements TelemetryAware {

    private final List<Expression> parameters;
    private final PromqlFunctionDefinition definition;

    public PromqlFunctionCall(Source source, LogicalPlan child, PromqlFunctionDefinition definition, List<Expression> parameters) {
        super(source, child);
        this.parameters = parameters != null ? parameters : List.of();
        this.definition = definition;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("PromqlFunctionCall does not support serialization");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("PromqlFunctionCall does not support serialization");
    }

    public String functionName() {
        return definition.name();
    }

    public List<Expression> parameters() {
        return parameters;
    }

    public PromqlFunctionDefinition definition() {
        return definition;
    }

    @Override
    public boolean expressionsResolved() {
        return Resolvables.resolved(parameters);
    }

    // @Override
    // public String telemetryLabel() {
    // return "PROMQL_FUNCTION_CALL";
    // }

    @Override
    public int hashCode() {
        return Objects.hash(child(), parameters, definition);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        PromqlFunctionCall other = (PromqlFunctionCall) obj;
        return Objects.equals(child(), other.child())
            && Objects.equals(parameters, other.parameters)
            && Objects.equals(definition, other.definition);
    }

    @Override
    public List<Attribute> output() {
        return List.of();
    }

    /**
     * Builds the ES|QL expression that implements this PromQL function call.
     * <p>
     * The builder returns an {@link Expression}: a value expression for scalar/aggregate/value-transformation
     * functions, or an {@code Order} (possibly {@code null} when unordered) for the order-statistic reductions
     * ({@code topk}, {@code bottomk}, {@code limitk}), consumed by the translator. Functions lowered to plan nodes
     * instead ({@code limit_ratio}) are translated directly and their builders throw.
     *
     * @param target the primary input expression (child vector or scalar), or {@code null} for zero-argument functions
     * @param ctx    the PromQL evaluation context (timestamp, window, step, configuration)
     */
    public Expression buildEsqlFunction(Expression target, PromqlFunctionRegistry.PromqlContext ctx) {
        try {
            // PromQL accepts any numeric range vector. ES|QL distinguishes counter from gauge types
            // internally, so plain numerics are wrapped with to_counter() for counter-required
            // functions and counter metrics are wrapped with to_gauge() for gauge-only functions.
            if (target != null && target.resolved() && target.dataType().isHistogram() == false) {
                var counterSupport = definition.counterSupport();
                if (counterSupport == PromqlFunctionDefinition.CounterSupport.REQUIRED && DataType.isCounter(target.dataType()) == false) {
                    target = new ToCounter(source(), target);
                } else if (counterSupport == PromqlFunctionDefinition.CounterSupport.UNSUPPORTED && DataType.isCounter(target.dataType())) {
                    target = new ToGauge(source(), target);
                }
            }
            return definition.esqlBuilder().build(source(), target, ctx, parameters());
        } catch (Exception e) {
            throw new ParsingException(source(), "Error building ESQL function for [{}]: {}", functionName(), e.getMessage());
        }
    }

    public abstract FunctionType functionType();

    /**
     * {@inheritDoc}
     * <p>
     * Re-declared abstract on the {@link PromqlFunctionCall} hierarchy so every PromQL function node classifies itself
     * explicitly instead of silently inheriting the transparent default: adding a new function node fails to compile until
     * its relabel-placement semantics are decided.
     */
    @Override
    public abstract boolean isIdentityTransparent();

    /**
     * Bypass a known-empty input in the generic lowering. Functions that synthesize rows from empty input need their
     * own lowering. The legacy path also bypasses nonempty compile-time relations; the typed path evaluates them.
     */
    protected final boolean skipFunctionTranslation(TranslationContext context, IntermediateResult input) {
        return input.kind() == IntermediateResult.Kind.EMPTY || (input.kind().constant && supportTimeSeriesMetadataType(context) == false);
    }

    @Override
    public final PromqlDataType returnType() {
        return functionType().outputType;
    }

    /**
     * Translates a generic PromQL function call (rate, ceil, abs, etc.) into an expression over the child's value.
     * Shared by the function families that need no special lowering; each still declares its own
     * {@link #translate} so a new family cannot inherit this lowering by accident.
     */
    protected final IntermediateResult translateValueFunction(TranslationContext context) {
        IntermediateResult child = context.translate(child());
        if (skipFunctionTranslation(context, child)) {
            return child;
        }
        Expression window = AggregateFunction.NO_WINDOW;
        if (child() instanceof RangeSelector rangeSelector) {
            window = context.cmd().resolveRangeWindow(rangeSelector.range());
        }
        var promqlCtx = new PromqlContext(context.time(), window, child.step(), context.configuration());
        return context.eval(child, buildEsqlFunction(child.value(), promqlCtx));
    }

    /**
     * Value and range functions remove the metric name after evaluating each input series. Finish the original
     * identity first: two series differing only by name must raise a duplicate-series error, never merge silently.
     */
    protected final IntermediateResult translateValueFunctionDroppingMetricName(TranslationContext context) {
        if (supportTimeSeriesMetadataType(context) == false || metricNameMayBePresent(context) == false) {
            return translateValueFunction(context);
        }
        TranslationContext fullIdentity = context.withRequired(union(context.required(), open()));
        IntermediateResult result = translateValueFunction(fullIdentity);
        if (result.kind() == IntermediateResult.Kind.EMPTY) {
            return result;
        }
        result = fullIdentity.inlineConstantSteps(result);
        if (result.kind().afterInitialAggregation == false) {
            result = fullIdentity.collapse(result, result.schema(), result.value());
        }
        if (result.schema().isOpen() == false && result.schema().labels().contains(LabelMatcher.NAME) == false) {
            return result;
        }
        var definitions = new ArrayList<Alias>();
        var bindings = new HashMap<>(result.labelBindings());
        Attribute name = result.labelColumn(LabelMatcher.NAME);
        if (name != null) {
            // Retain a declared finite column as null: null labels are absent, and the public output shape remains stable.
            Alias absent = new Alias(source(), name.name(), new Literal(source(), null, name.dataType()));
            definitions.add(absent);
            bindings.put(LabelMatcher.NAME, absent.toAttribute());
        } else {
            definitions.add(
                new Alias(
                    source(),
                    MetadataAttribute.TIMESERIES,
                    new TimeSeriesUnset(source(), result.metadata(), List.of(Literal.keyword(source(), LabelMatcher.NAME)))
                )
            );
        }
        LogicalPlan plan = new Eval(source(), result.plan(), definitions);
        return PromqlPlan.checkUnique(source(), result.with(plan, result.schema(), result.value(), bindings));
    }

    private boolean metricNameMayBePresent(TranslationContext context) {
        if (child() instanceof Selector) {
            // PROMQL requests all dimension fields from field-caps. A flattened field can hide individual keys,
            // so only a fully resolved source without either NAME or flattened fields proves the name absent.
            return context.cmd()
                .child()
                .output()
                .stream()
                .anyMatch(
                    attribute -> attribute.resolved() == false
                        || attribute instanceof FieldAttribute field
                            && (LabelMatcher.NAME.equals(PromqlLabels.labelName(field))
                                || field.dataType() == DataType.FLATTENED
                                || field.dataType() == DataType.UNSUPPORTED
                                || field.field() instanceof UnionTypeEsField)
                );
        }
        if (PromqlLabels.hasTsMetadata(child().output()) == false) {
            return child().output().stream().anyMatch(attribute -> LabelMatcher.NAME.equals(PromqlLabels.labelName(attribute)));
        }
        return true;
    }
}
