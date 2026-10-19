/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql.selector;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.capabilities.Resolvables;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.TimeSeriesMetadataAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.Case;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.TemporaryNameGenerator;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.promql.PlaceholderRelation;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlLabels;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlPlan;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContext;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContext.IntermediateResult;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContext.IntermediateResult.Kind;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationSchema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeSet;

import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContext.supportTimeSeriesMetadataType;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationSchema.project;

/**
 * Base class representing a PromQL vector selector.
 * A vector selector is defined by a set of label matchers and a point in time evaluation context.
 */
public abstract sealed class Selector extends UnaryPlan implements PromqlPlan permits InstantSelector, RangeSelector, LiteralSelector {
    // implements TelemetryAware

    // in Promql this is the __name__ label however for now, this gets mapped to an exact field
    private final Expression series;
    private final List<Expression> labels;
    private final LabelMatchers labelMatchers;
    private final Evaluation evaluation;
    protected List<Attribute> output;

    Selector(Source source, Expression series, List<Expression> labels, LabelMatchers labelMatchers, Evaluation evaluation) {
        this(source, PlaceholderRelation.INSTANCE, series, labels, labelMatchers, evaluation);
    }

    Selector(
        Source source,
        LogicalPlan child,
        Expression series,
        List<Expression> labels,
        LabelMatchers labelMatchers,
        Evaluation evaluation
    ) {
        super(source, child);
        this.series = series;
        this.labels = labels;
        this.labelMatchers = labelMatchers;
        this.evaluation = evaluation;
    }

    public Expression series() {
        return series;
    }

    public List<Expression> labels() {
        return labels;
    }

    public LabelMatchers labelMatchers() {
        return labelMatchers;
    }

    public Evaluation evaluation() {
        return evaluation;
    }

    @Override
    public boolean expressionsResolved() {
        return (series == null || series.resolved()) && Resolvables.resolved(labels);
    }

    @Override
    public boolean equals(Object o) {
        if (super.equals(o)) {
            Selector selector = (Selector) o;
            return Objects.equals(evaluation, selector.evaluation)
                && Objects.equals(labelMatchers, selector.labelMatchers)
                && Objects.equals(series, selector.series)
                && Objects.equals(labels, selector.labels);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), series, labels, labelMatchers, evaluation);
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("should not serialize");
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("should not serialize");
    }

    /**
     * Translates a source-backed selector (instant or range) reading {@code value} per series; label matchers lower to a
     * pending filter predicate. Shared by the selectors that read the source relation; each still declares its own
     * {@link #translate} so a new selector cannot inherit this lowering by accident.
     */
    protected final IntermediateResult translateSeries(TranslationContext context, Expression value) {
        LogicalPlan input = context.cmd().child();
        Expression matcher = labelMatchers().predicate(source(), labels(), context.configuration());

        if (input instanceof EsRelation relation && relation.concreteQualifiedIndices().isEmpty()) {
            var empty = new LocalRelation(
                context.cmd().source(),
                // Legacy constant propagation retains this empty relation through the command's final projection.
                supportTimeSeriesMetadataType(context)
                    ? List.of(context.cmd().valueAttribute(), context.cmd().stepAttribute())
                    : context.cmd().output(),
                EmptyLocalSupplier.EMPTY
            );
            return new IntermediateResult(
                empty,
                TranslationSchema.EMPTY,
                Literal.NULL,
                context.cmd().stepAttribute(),
                null,
                supportTimeSeriesMetadataType(context) ? Kind.EMPTY : Kind.CONSTANT,
                supportTimeSeriesMetadataType(context) ? Map.of() : null
            );
        }

        List<Attribute> dimensions = input.output()
            .stream()
            .filter(attribute -> attribute instanceof FieldAttribute field && field.isDimension())
            .filter(attribute -> attribute instanceof TimeSeriesMetadataAttribute == false)
            .toList();
        // Expose only required labels that exist on the relation. Consumers null-fill any required label that is absent.
        TranslationSchema schema = project(context.required(), PromqlLabels.labelNames(dimensions));
        Map<String, Attribute> labelBindings = null;
        if (supportTimeSeriesMetadataType(context)) {
            var excludedFields = new TreeSet<String>();
            for (Attribute dimension : dimensions) {
                if (schema.labels().contains(PromqlLabels.labelName(dimension))) {
                    excludedFields.add(((FieldAttribute) dimension).fieldName().string());
                }
            }
            boolean needsMetadata = schema.isOpen() || schema.labels().containsAll(context.required().labels()) == false;
            var metadata = needsMetadata
                ? TimeSeriesMetadataAttribute.create(source(), excludedFields, context.analyzer().minimumVersion())
                : null;
            input = context.pushDownEvaluationTimestampFilter(input, this).transformUp(EsRelation.class, relation -> {
                var attributes = new ArrayList<>(relation.output());
                attributes.removeIf(a -> a instanceof TimeSeriesMetadataAttribute);
                if (metadata != null) {
                    attributes.add(metadata);
                }
                return relation.withAttributes(attributes);
            });
            if (metadata != null) {
                schema = TranslationSchema.union(schema, TranslationSchema.open());
            }
            // Physical names are private to the plan. A label named "step" or "_timeseries" must not
            // replace an execution column; the schema retains the logical name independently.
            labelBindings = new HashMap<>();
            var promoted = new ArrayList<Alias>();
            for (String name : schema.labels()) {
                Attribute dimension = TranslationContext.find(dimensions, name);
                Expression labelValue = dimension;
                if (DataType.isString(dimension.dataType())) {
                    // PromQL treats an empty label as absent, including labels promoted out of metadata.
                    labelValue = new Case(
                        source(),
                        new Equals(source(), dimension, Literal.keyword(source(), "")),
                        List.of(new Literal(source(), null, DataType.KEYWORD), dimension)
                    );
                }
                Alias alias = new Alias(source(), TemporaryNameGenerator.locallyUniqueTemporaryName("label"), labelValue);
                promoted.add(alias);
                labelBindings.put(name, alias.toAttribute());
            }
            if (promoted.isEmpty() == false) {
                input = new Eval(source(), input, promoted);
            }
        }
        return new IntermediateResult(input, schema, value, context.stepAttr(), matcher, Kind.BEFORE_INITIAL_AGGREGATE, labelBindings);
    }
}
