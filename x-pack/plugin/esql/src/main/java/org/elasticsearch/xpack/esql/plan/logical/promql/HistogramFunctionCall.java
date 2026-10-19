/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Values;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDouble;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesUnset;
import org.elasticsearch.xpack.esql.expression.promql.function.FunctionType;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionDefinition;
import org.elasticsearch.xpack.esql.plan.logical.Drain;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContext.IntermediateResult;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.LabelMatcher;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.esql.plan.logical.promql.PromqlLabels.isTsMetadata;
import static org.elasticsearch.xpack.esql.plan.logical.promql.PromqlLabels.labelName;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContext.supportTimeSeriesMetadataType;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationSchema.finite;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationSchema.open;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationSchema.project;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationSchema.subtract;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationSchema.union;

/**
 * Base class for PromQL histogram functions that evaluate classic histogram buckets grouped by their {@code le} label.
 */
public abstract sealed class HistogramFunctionCall extends PromqlFunctionCall permits HistogramFraction, HistogramQuantile {
    public static final String LE_LABEL = "le";

    private List<Attribute> output;

    protected HistogramFunctionCall(Source source, LogicalPlan child, PromqlFunctionDefinition definition, List<Expression> parameters) {
        super(source, child, definition, parameters);
    }

    /**
     * Builds the aggregate expression for this function call:
     * The aggregate expression will be invoked with the bucket counts and their upper bounds (the le labels).
     */
    public abstract Expression buildAggregateFunction(Expression count, Expression upperBound);

    @Override
    public final List<Attribute> output() {
        if (output == null) {
            output = child().output().stream().filter(attr -> isTsMetadata(attr) || LE_LABEL.equals(labelName(attr)) == false).toList();
        }
        return output;
    }

    @Override
    public final FunctionType functionType() {
        return FunctionType.HISTOGRAM;
    }

    @Override
    public boolean isIdentityTransparent() {
        // Reshapes labels (drops `le`) but is not a grouping boundary for relabel placement: a relabel below it still
        // feeds the enclosing aggregation.
        return true;
    }

    @Override
    public Map<String, DataType> labelTypes() {
        var types = new HashMap<>(PromqlPlan.labelTypes(child()));
        types.remove(LE_LABEL);
        types.remove(LabelMatcher.NAME);
        return Map.copyOf(types);
    }

    @Override
    public IntermediateResult translate(TranslationContext context) {
        // Classic histogram functions collapse the `le` bucket dimension like a `without (le)` would, and read the
        // bucket bound off the `le` column itself, so the child must also expose it by name.
        List<String> le = List.of(HistogramFunctionCall.LE_LABEL);
        TranslationSchema childRequired = union(union(subtract(context.required(), le), open(le)), finite(le));
        IntermediateResult result = context.withRequired(childRequired).translate(child());
        if (skipFunctionTranslation(context, result)) {
            return result;
        }

        // native histograms - distinguishable only at this point in planning are regular value transformations.
        if (result.value().resolved() && result.value().dataType().isHistogram()) {
            return new ValueTransformationFunction(source(), child(), definition(), parameters()).translate(context);
        }

        // Classic counter-backed histograms need the special treatment below.
        Attribute leColumn = result.label(HistogramFunctionCall.LE_LABEL);
        if (leColumn == null) {
            // Prometheus ignores float samples without a bucket bound. Preserve the legacy diagnostic only.
            if (supportTimeSeriesMetadataType(context)) {
                result = context.inlineConstantSteps(result);
                if (result.kind().afterInitialAggregation == false) {
                    result = context.collapse(result, result.schema(), result.value());
                }
                // Ignoring samples must still evaluate the argument, including duplicate-series checks below it.
                return result.with(new Drain(source(), result.plan()), result.schema(), result.value());
            }
            HeaderWarning.addWarning(functionName() + ": input vector has no le label; no buckets to evaluate");
            var skipAllFilter = new Filter(source(), result.plan(), Literal.FALSE);
            var nullGrouping = new Values(source(), new Literal(source(), null, DataType.DOUBLE));
            IntermediateResult skipped = result.with(skipAllFilter, result.schema(), result.value());
            return skipped.kind().afterInitialAggregation
                ? context.regroup(skipped, result.schema(), false, nullGrouping)
                : context.collapse(skipped, result.schema(), nullGrouping);
        }

        if (result.kind().afterInitialAggregation == false) {
            result = context.collapse(result, result.schema(), result.value());
            leColumn = result.label(HistogramFunctionCall.LE_LABEL);
            assert leColumn != null : "invariant: [ " + HistogramFunctionCall.LE_LABEL + " ] required";
        }

        // Bucket counts are consumed as doubles; counter buckets are frequently integer/long typed, so cast explicitly.
        if (supportTimeSeriesMetadataType(context)) {
            var excluded = Set.of(LE_LABEL, LabelMatcher.NAME);
            var metadataLabels = result.schema().labelsInMetadata(excluded);
            LogicalPlan plan = result.plan();
            if (metadataLabels.isEmpty() == false) {
                var names = metadataLabels.stream().map(name -> (Expression) Literal.keyword(source(), name)).toList();
                var metadata = new TimeSeriesUnset(source(), result.metadata(), names);
                plan = new Eval(source(), plan, List.of(new Alias(source(), MetadataAttribute.TIMESERIES, metadata)));
            }
            var remaining = new LinkedHashSet<>(result.schema().labels());
            remaining.removeAll(excluded);
            result = result.with(plan, project(result.schema(), remaining), result.value());
        }
        TranslationSchema schema = context.regroupWithout(result.schema(), le);
        Expression count = new ToDouble(source(), result.value());
        return context.regroup(result, schema, true, buildAggregateFunction(count, leColumn));
    }
}
