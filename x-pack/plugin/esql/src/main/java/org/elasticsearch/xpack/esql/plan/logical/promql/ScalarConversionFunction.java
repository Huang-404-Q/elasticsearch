/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Scalar;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDouble;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.promql.function.FunctionType;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionDefinition;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.TemporaryNameGenerator;
import org.elasticsearch.xpack.esql.parser.promql.PromqlLogicalPlanBuilder;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.KeepMinimum;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.PackDims;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContext.IntermediateResult;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContext.IntermediateResult.Kind;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContext.supportTimeSeriesMetadataType;

/**
 * Represents the {@code scalar(v instant-vector)} PromQL function call that converts a vector to a scalar.
 */
public final class ScalarConversionFunction extends PromqlFunctionCall {

    public ScalarConversionFunction(Source source, LogicalPlan child, PromqlFunctionDefinition definition, List<Expression> parameters) {
        super(source, child, definition, parameters);
    }

    @Override
    public FunctionType functionType() {
        return FunctionType.SCALAR_CONVERSION;
    }

    @Override
    public boolean isIdentityTransparent() {
        return true;
    }

    @Override
    protected NodeInfo<PromqlFunctionCall> info() {
        return NodeInfo.create(this, ScalarConversionFunction::new, child(), definition(), parameters());
    }

    @Override
    public ScalarConversionFunction replaceChild(LogicalPlan newChild) {
        return new ScalarConversionFunction(source(), newChild, definition(), parameters());
    }

    @Override
    public List<Attribute> output() {
        return List.of();
    }

    /** scalar(): collapse to one value per step, e.g. scalar(sum by (cluster) (metric)). */
    @Override
    public IntermediateResult translate(TranslationContext context) {
        if (supportTimeSeriesMetadataType(context)) {
            return translatePerStep(context);
        }
        // The result has no labels, so the child's label set is irrelevant: it exposes none.
        IntermediateResult child = context.withRequired(TranslationSchema.EMPTY).translate(child());
        if (child.value().foldable()) {
            Expression value = new ToDouble(source(), child.value());
            return new IntermediateResult(child.plan(), TranslationSchema.EMPTY, value, child.step(), child.pendingFilter());
        }
        var scalarExpr = new Scalar(source(), child.value());
        return child.kind().afterInitialAggregation
            ? context.regroup(child, TranslationSchema.EMPTY, false, scalarExpr)
            : context.collapse(child, TranslationSchema.EMPTY, scalarExpr);
    }

    private IntermediateResult translatePerStep(TranslationContext context) {
        // Finish the vector before removing its labels: two different series with equal samples still count as two.
        IntermediateResult child = context.withRequired(TranslationSchema.open())
            .translateIntermediate(child(), new NameId(), new NameId());
        LogicalPlan present = new Filter(source(), child.plan(), new IsNotNull(source(), child.value()));
        // PromQL ignores histogram samples. Keep the child plan so relabeling still checks for duplicate series.
        Expression sample = child.value().resolved() && child.value().dataType().isHistogram()
            ? new Literal(source(), null, DataType.DOUBLE)
            : child.value();
        IntermediateResult scalar = context.regroup(
            child.with(present, child.schema(), child.value()),
            TranslationSchema.EMPTY,
            false,
            new Scalar(source(), sample)
        );

        // A grouped aggregate has no row for an absent step. Supply NaN on the evaluation clock, then prefer real rows.
        Literal nan = new Literal(source(), Double.NaN, DataType.DOUBLE);
        IntermediateResult defaults;
        if (context.cmd().start().value() != null) {
            LogicalPlan steps = PromqlLogicalPlanBuilder.buildLocalRelation(context.cmd(), context.foldContext());
            defaults = new IntermediateResult(
                steps,
                TranslationSchema.EMPTY,
                nan,
                steps.output().getFirst(),
                null,
                Kind.CONSTANT,
                Map.of()
            );
        } else {
            defaults = context.sourceSteps(context.cmd().promqlPlan(), nan);
        }
        String priorityName = TemporaryNameGenerator.locallyUniqueTemporaryName("scalar_priority");
        List<LogicalPlan> branches = List.of(branch(context, scalar, priorityName, 0), branch(context, defaults, priorityName, 1));
        // The defaults define the fixed scalar/step/priority types even when the input awaits a verifier diagnostic.
        // A union owns its column identities; branch-local constants must not fold through the merge.
        List<Attribute> columns = Expressions.toReferenceAttributesPreservingIds(branches.getLast().output(), List.of());
        LogicalPlan plan = new UnionAll(source(), branches, columns);
        Attribute step = TranslationContext.find(columns, context.cmd().stepColumnName());
        Attribute value = TranslationContext.find(columns, context.cmd().valueColumnName());
        Attribute priority = TranslationContext.find(columns, priorityName);
        Attribute key = PackDims.newPackedAttribute(source());
        plan = new PackDims(source(), plan, List.of(step), key);
        plan = new KeepMinimum(source(), plan, key, priority);
        Alias resultStep = new Alias(source(), context.cmd().stepColumnName(), step, context.stepAttr().id());
        plan = new Project(source(), new Eval(source(), plan, List.of(resultStep)), List.of(value, resultStep.toAttribute()));
        return new IntermediateResult(
            plan,
            TranslationSchema.EMPTY,
            value,
            resultStep.toAttribute(),
            null,
            Kind.AFTER_INITIAL_AGGREGATE,
            Map.of()
        );
    }

    private LogicalPlan branch(TranslationContext context, IntermediateResult input, String priorityName, int priority) {
        List<Alias> columns = List.of(
            new Alias(source(), context.cmd().valueColumnName(), input.value()),
            new Alias(source(), context.cmd().stepColumnName(), input.step()),
            new Alias(source(), priorityName, new Literal(source(), priority, DataType.INTEGER))
        );
        return new Project(source(), new Eval(source(), input.plan(), columns), columns.stream().map(Alias::toAttribute).toList());
    }
}
