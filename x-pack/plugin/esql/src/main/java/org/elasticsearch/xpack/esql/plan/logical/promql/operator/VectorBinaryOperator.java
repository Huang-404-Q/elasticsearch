/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql.operator;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.expression.function.Function;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDouble;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesGet;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesHash;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesUnset;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.TemporaryNameGenerator;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.BinaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.PackDims;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.join.InnerJoin;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlCommand;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlDataType;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlLabels;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlPlan;
import org.elasticsearch.xpack.esql.plan.logical.promql.ScalarConversionFunction;
import org.elasticsearch.xpack.esql.plan.logical.promql.ScalarFunction;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContext;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContext.IntermediateResult;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContext.IntermediateResult.Kind;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationSchema;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.LabelMatcher;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

import static org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction.withFilter;
import static org.elasticsearch.xpack.esql.expression.predicate.Predicates.combineAndNullable;
import static org.elasticsearch.xpack.esql.plan.logical.promql.PromqlDataType.SCALAR;
import static org.elasticsearch.xpack.esql.plan.logical.promql.PromqlLabels.hasTsMetadata;
import static org.elasticsearch.xpack.esql.plan.logical.promql.PromqlPlan.getType;
import static org.elasticsearch.xpack.esql.plan.logical.promql.PromqlPlan.returnsScalar;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContext.supportTimeSeriesMetadataType;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationSchema.finite;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationSchema.open;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationSchema.project;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationSchema.subtract;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationSchema.union;
import static org.elasticsearch.xpack.esql.plan.logical.promql.operator.VectorMatch.Joining;

public abstract sealed class VectorBinaryOperator extends BinaryPlan implements PromqlPlan permits VectorBinarySet, VectorBinaryComparison,
    VectorBinaryArithmetic {

    /**
     * Give the build operand fresh identities for self-joins. Rename its value because {@link InnerJoin#output()}
     * merges columns by name and would otherwise replace the probe value.
     */
    record PlanRefresher(String valueName, String refreshedValueName, Map<NameId, NameId> mapper) {
        PlanRefresher(String valueName) {
            this(valueName, TemporaryNameGenerator.locallyUniqueTemporaryName(valueName), new HashMap<>());
        }

        private Expression refresh(Expression expression) {
            if (expression instanceof NamedExpression ne) {
                String name = ne.name().equals(valueName) ? refreshedValueName : ne.name();
                NameId id = mapper.computeIfAbsent(ne.id(), k -> new NameId());
                if (ne instanceof Attribute attribute) {
                    return attribute.withName(name).withId(id);
                }
                if (ne instanceof Alias alias) {
                    return new Alias(alias.source(), name, alias.child(), id, alias.synthetic());
                }
            }
            return expression;
        }

        private LogicalPlan refreshPlan(LogicalPlan plan) {
            // MergePlan equality ignores its stored output identities. Rebuild every node explicitly so refreshing
            // that output is not discarded while its branches and the enclosing references receive fresh identities.
            if (plan.children().isEmpty() == false) {
                plan = plan.replaceChildren(plan.children().stream().map(this::refreshPlan).toList());
            }
            return plan.transformExpressionsOnly(this::refresh);
        }

        IntermediateResult apply(IntermediateResult input) {
            Map<String, Attribute> bindings = input.labelBindings() == null ? null : new HashMap<>();
            if (bindings != null) {
                input.labelBindings().forEach((name, attribute) -> bindings.put(name, (Attribute) refresh(attribute)));
            }
            return new IntermediateResult(
                refreshPlan(input.plan()),
                input.schema(),
                refresh(input.valueColumn()),
                (Attribute) refresh(input.step()),
                input.pendingFilter(),
                input.kind(),
                bindings
            );
        }
    }

    private final VectorMatch match;
    private final boolean dropMetricName;
    private final BinaryOp binaryOp;
    private List<Attribute> output;

    /** One side of the join */
    record PreparedJoinInput(LogicalPlan plan, List<Attribute> keys) {}

    /** Projected columns and their logical label identities on the typed path. */
    private record BoundOutput(List<NamedExpression> columns, Map<String, Attribute> labels) {}

    /**
     * Underlying binary operation (e.g. +, -, *, /, etc.) being performed
     * on the actual values of the vectors.
     */
    public interface BinaryOp {
        String name();

        ScalarFunctionFactory asFunction();
    }

    public interface ScalarFunctionFactory {
        Function create(Source source, Expression left, Expression right, Configuration configuration);
    }

    protected VectorBinaryOperator(
        Source source,
        LogicalPlan left,
        LogicalPlan right,
        VectorMatch match,
        boolean dropMetricName,
        BinaryOp binaryOp
    ) {
        super(source, left, right);
        this.match = match;
        this.dropMetricName = dropMetricName;
        this.binaryOp = binaryOp;
    }

    /** The declared vector matching; {@link VectorMatch#NONE} - never null - when the operator declares none. */
    public VectorMatch match() {
        return match;
    }

    public BinaryOp binaryOp() {
        return binaryOp;
    }

    @Override
    public List<Attribute> output() {
        if (output == null) {
            output = computeOutput();
        }
        return output;
    }

    @Override
    public Map<String, DataType> labelTypes() {
        boolean leftScalar = returnsScalar(left());
        boolean rightScalar = returnsScalar(right());
        boolean keepRight = leftScalar || match.grouping() == Joining.RIGHT;
        var types = new HashMap<>(PromqlPlan.labelTypes(keepRight ? right() : left()));
        if (leftScalar == false && rightScalar == false) {
            if (match.grouping() != Joining.NONE) {
                Map<String, DataType> copied = PromqlPlan.labelTypes(keepRight ? left() : right());
                for (String name : match.groupingLabels()) {
                    types.remove(name);
                    if (copied.containsKey(name)) {
                        types.put(name, copied.get(name));
                    }
                }
            } else if (match.filter() == VectorMatch.Filter.ON) {
                types.keySet().retainAll(match.filterLabels());
            } else if (match.filter() == VectorMatch.Filter.IGNORING) {
                types.keySet().removeAll(match.filterLabels());
            }
        }
        if (dropMetricName) {
            types.remove(LabelMatcher.NAME);
        }
        return Map.copyOf(types);
    }

    private List<Attribute> computeOutput() {
        // Between an instant vector and a scalar,
        // the operator is applied to the value of every data sample in the vector.
        // Therefore, we're returning any grouping attributes (like those created for by (...) and _timeseries) from the vector.
        // If both the left and right are a scalar, this works, too
        // as both outputs will be empty (scalars don't have any grouping attributes).
        if (returnsScalar(left())) {
            return right().output();
        }
        if (returnsScalar(right())) {
            return left().output();
        }
        Set<String> outputLabels;
        List<Attribute> leftAttrs = left().output();
        List<Attribute> rightAttrs = right().output();
        Set<String> leftLabels = extractLabelNames(leftAttrs);
        Set<String> rightLabels = extractLabelNames(rightAttrs);
        Attribute metadata = null;
        if (match.grouping() != Joining.NONE) {
            // Keep the many side's labels and copy only explicitly included labels from the one side.
            boolean manyRight = match.grouping() == Joining.RIGHT;
            List<Attribute> many = manyRight ? rightAttrs : leftAttrs;
            if (match.groupingLabels().isEmpty() && hasTsMetadata(many)) {
                // Matching reads named keys, but the result retains the many side's runtime identity.
                return many;
            }
            metadata = many.stream().filter(PromqlLabels::isTsMetadata).findFirst().orElse(null);
            outputLabels = new HashSet<>(manyRight ? rightLabels : leftLabels);
            outputLabels.addAll(match.groupingLabels());
            if (match.filter() == VectorMatch.Filter.ON) {
                outputLabels.addAll(match.filterLabels());
            }
        } else if (match.filter() == VectorMatch.Filter.ON) {
            outputLabels = new HashSet<>(match.filterLabels());
        } else if (match.filter() == VectorMatch.Filter.IGNORING) {
            outputLabels = new HashSet<>(leftLabels);
            outputLabels.removeAll(match.filterLabels());
            metadata = leftAttrs.stream().filter(PromqlLabels::isTsMetadata).findFirst().orElse(null);
        } else if (leftLabels.equals(rightLabels)) {
            return leftAttrs;
        } else {
            // Default matching between different label sets: a pair matches only where the labels one side lacks are
            // absent on the other side too (a Prometheus signature has no entry for an absent label), and like every
            // one-to-one match the result carries the left operand's labels.
            outputLabels = new HashSet<>(leftLabels);
            metadata = leftAttrs.stream().filter(PromqlLabels::isTsMetadata).findFirst().orElse(null);
        }

        if (dropMetricName) {
            outputLabels.remove(LabelMatcher.NAME);
        }

        boolean manyRight = match.grouping() == Joining.RIGHT;
        List<Attribute> carrying = manyRight ? rightAttrs : leftAttrs;
        List<Attribute> other = manyRight ? leftAttrs : rightAttrs;
        List<Attribute> result = new ArrayList<>();
        if (metadata != null) {
            result.add(metadata);
            // A literal label with this name stays inside the dictionary; the carrier is a different logical role.
            outputLabels.remove(PromqlLabels.labelName(metadata));
        }
        for (String label : outputLabels) {
            Attribute attr = findAttribute(label, carrying, other);
            if (match.groupingLabels().contains(label)) {
                // Included labels replace the many side's value and type with the one side's, even when absent there.
                attr = findAttribute(label, other, List.of());
            }
            if (attr == null) {
                // Matching and grouping can request labels neither operand declares; translation materializes or null-fills them.
                attr = new ReferenceAttribute(source(), label, labelTypes().getOrDefault(label, DataType.KEYWORD));
            }
            result.add(attr);
        }

        return result;
    }

    private HashSet<String> extractLabelNames(List<Attribute> attrs) {
        // The metadata carrier is not a label; sample values are carried separately by IntermediateResult.
        return new HashSet<>(namedLabels(attrs));
    }

    private Attribute findAttribute(String name, List<Attribute> left, List<Attribute> right) {
        for (var attr : left) {
            if (PromqlLabels.isTsMetadata(attr) == false && PromqlLabels.labelName(attr).equals(name)) {
                return attr;
            }
        }
        for (var attr : right) {
            if (PromqlLabels.isTsMetadata(attr) == false && PromqlLabels.labelName(attr).equals(name)) {
                return attr;
            }
        }
        return null;
    }

    @Override
    public abstract VectorBinaryOperator replaceChildren(LogicalPlan newLeft, LogicalPlan newRight);

    @Override
    public boolean expressionsResolved() {
        return true;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o)) {
            VectorBinaryOperator that = (VectorBinaryOperator) o;
            return dropMetricName == that.dropMetricName && Objects.equals(match, that.match) && Objects.equals(binaryOp, that.binaryOp);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), match, dropMetricName, binaryOp);
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("PromQL plans should not be serialized");
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("PromQL plans should not be serialized");
    }

    @Override
    public PromqlDataType returnType() {
        PromqlDataType leftType = getType(left());
        PromqlDataType rightType = getType(right());
        // scalar op scalar -> scalar; otherwise -> vector
        if (leftType == SCALAR && rightType == SCALAR) {
            return SCALAR;
        }
        return PromqlDataType.INSTANT_VECTOR;
    }

    @Override
    public boolean isIdentityTransparent() {
        // Matches and merges two operands' series identities: a relabel below either operand feeds this boundary.
        return false;
    }

    /** Whether the operands can compose over a shared aggregate. */
    public boolean canTranslateAsAggregate(TranslationContext context) {
        if (returnsScalar(left()) || returnsScalar(right())) {
            return true;
        }
        return requiresHashJoin(context) == false
            && hasBinaryOperatorSubtree(left(), context) == false
            && hasBinaryOperatorSubtree(right(), context) == false;
    }

    /** Whether this operator's own matching rules require a join, independent of its descendants. */
    private boolean requiresHashJoin(TranslationContext context) {
        if (returnsScalar(left()) || returnsScalar(right())) {
            return false;
        }

        if (match.filter() != VectorMatch.Filter.NONE || match.grouping() != Joining.NONE) {
            return true;
        }
        if (supportTimeSeriesMetadataType(context) && this instanceof VectorBinaryComparison comparison && comparison.filterMode()) {
            return true;
        }

        var left = left().output();
        var right = right().output();

        boolean leftHasTsMetadata = hasTsMetadata(left);
        boolean rightHasTsMetadata = hasTsMetadata(right);
        if (leftHasTsMetadata || rightHasTsMetadata) {
            return supportTimeSeriesMetadataType(context);
        }

        return extractLabelNames(left).equals(extractLabelNames(right)) == false;
    }

    private static boolean hasBinaryOperatorSubtree(LogicalPlan plan, TranslationContext context) {
        // Check local matching rules only: calling canTranslateAsAggregate() here would repeatedly traverse the same subtrees.
        // Scalar and set operators need no matching join themselves, but their descendants can still contain one.
        boolean metadataTypeSupported = supportTimeSeriesMetadataType(context);
        return plan.anyMatch(
            p -> metadataTypeSupported && p instanceof VectorBinarySet
                || p instanceof VectorBinaryOperator vbo && vbo instanceof VectorBinarySet == false
                // Preserve legacy composition: only explicit matching made an ancestor use a join.
                    && (metadataTypeSupported
                        ? vbo.requiresHashJoin(context)
                        : vbo.match.filter() != VectorMatch.Filter.NONE || vbo.match.grouping() != Joining.NONE)
        );
    }

    /** Translates vector matching as a join; compatible operands compose over a shared aggregate. */
    @Override
    public IntermediateResult translate(TranslationContext context) {
        if (supportTimeSeriesMetadataType(context) && (hasMaterializedScalar(left()) || hasMaterializedScalar(right()))) {
            return translateScalarOperator(context);
        }
        if (canTranslateAsAggregate(context)) {
            return translateAggregate(context);
        }
        return translateJoinOperator(context);
    }

    private static boolean hasMaterializedScalar(LogicalPlan operand) {
        return returnsScalar(operand)
            && operand.anyMatch(plan -> plan instanceof ScalarConversionFunction || plan instanceof ScalarFunction);
    }

    /**
     * Scalar conversions and source-free functions produce their own table with one value per evaluation step.
     * Broadcast that table by step; vector labels never participate in scalar matching or aggregate fusion.
     */
    private IntermediateResult translateScalarOperator(TranslationContext context) {
        TranslationSchema required = union(context.required(), finite(namedLabels(output())));
        IntermediateResult left = translateOperand(context, left(), required, hasTsMetadata(left().output()));
        IntermediateResult right = translateOperand(context, right(), required, hasTsMetadata(right().output()));
        boolean scalarLeft = returnsScalar(left());
        IntermediateResult probe = scalarLeft ? right : left;
        IntermediateResult build = new PlanRefresher(context.cmd().valueColumnName()).apply(scalarLeft ? left : right);
        LogicalPlan join = new InnerJoin(
            source(),
            probe.plan(),
            new Project(source(), build.plan(), List.of(build.step(), build.valueColumn())),
            List.of(probe.step()),
            List.of(build.step()),
            List.of(build.valueColumn()),
            false,
            true
        );
        var labels = new TreeSet<>(probe.schema().labels());
        if (dropMetricName) {
            labels.remove(LabelMatcher.NAME);
        }
        TranslationSchema schema = project(probe.schema(), labels);
        BoundOutput output = bindOutput(context, schema, output(), probe, build);
        IntermediateResult result = projectFinal(
            context,
            schema,
            scalarLeft ? build.value() : probe.value(),
            scalarLeft ? probe.value() : build.value(),
            probe.step(),
            join,
            output
        );
        return PromqlPlan.checkUnique(source(), result);
    }

    /** Composes a binary operator as an expression over the operands' shared aggregate. */
    private IntermediateResult translateAggregate(TranslationContext context) {
        IntermediateResult left = context.translate(left());
        if (supportTimeSeriesMetadataType(context)
            && dropMetricName
            && left.schema().isOpen()
            && left.kind().afterInitialAggregation == false) {
            left = context.collapse(left, left.schema(), left.value());
        }
        Expression leftExpr = new ToDouble(left.value().source(), left.value());
        if (supportTimeSeriesMetadataType(context) == false && this instanceof VectorBinaryComparison comp && comp.filterMode()) {
            return left.with(left.plan(), left.schema(), leftExpr);
        }

        IntermediateResult right = context.translate(right());
        if (supportTimeSeriesMetadataType(context)
            && dropMetricName
            && right.schema().isOpen()
            && right.kind().afterInitialAggregation == false) {
            right = context.collapse(right, right.schema(), right.value());
        }
        Expression rightExpr = new ToDouble(right.value().source(), right.value());
        boolean seriesPair = left.kind().afterInitialAggregation == false
            && right.kind().afterInitialAggregation == false
            && getType(left()) != SCALAR
            && getType(right()) != SCALAR;
        if (seriesPair) {
            // Two range functions over one series pair sample by sample; a side that counts no sample has no element.
            leftExpr = new ToDouble(left.value().source(), TranslationContext.emptySampleCountAsNull(left.value()));
            rightExpr = new ToDouble(right.value().source(), TranslationContext.emptySampleCountAsNull(right.value()));
        }
        if (this instanceof VectorBinaryComparison comparison && comparison.filterMode()) {
            // Scalar filters must consume per-step values, including when nested below another operator.
            boolean scalarLeft = returnsScalar(left());
            IntermediateResult vector = scalarLeft ? right : left;
            if (vector.kind().afterInitialAggregation == false) {
                vector = context.collapse(vector, vector.schema(), vector.value());
            }
            Expression lhs = scalarLeft ? leftExpr : new ToDouble(source(), vector.value());
            Expression rhs = scalarLeft ? new ToDouble(source(), vector.value()) : rightExpr;
            Expression condition = binaryOp().asFunction().create(source(), lhs, rhs, context.configuration());
            return vector.with(new Filter(source(), vector.plan(), condition), vector.schema(), vector.value());
        }
        Expression binaryExpr = binaryOp().asFunction().create(source(), leftExpr, rightExpr, context.configuration());

        LogicalPlan plan;
        Expression filter;
        if (left.kind().afterInitialAggregation && right.kind().afterInitialAggregation) {
            if (supportTimeSeriesMetadataType(context) && canMergeAggregates(left.plan(), right.plan()) == false) {
                // Materialized clocks, nested aggregation and label rewrites have independent rows.
                // Match finished tables rather than merging aggregates across those boundaries.
                return returnsScalar(left()) || returnsScalar(right()) ? translateScalarOperator(context) : translateJoinOperator(context);
            }
            plan = mergeAggregateOperators(left, right);
            filter = null;
        } else {
            plan = left.kind().afterInitialAggregation ? left.plan() : right.plan();
            filter = combineAndNullable(Arrays.asList(left.pendingFilter(), right.pendingFilter()));
        }
        IntermediateResult labelSource = left.schema().equals(TranslationSchema.EMPTY) == false ? left : right;
        TranslationSchema schema = labelSource.schema();
        Kind kind = left.kind().afterInitialAggregation || right.kind().afterInitialAggregation
            ? Kind.AFTER_INITIAL_AGGREGATE
            : Kind.BEFORE_INITIAL_AGGREGATE;
        IntermediateResult result = new IntermediateResult(
            plan,
            schema,
            null,
            left.kind().afterInitialAggregation ? left.step() : right.step(),
            filter,
            kind,
            labelSource.retainedBindings(plan, schema)
        );
        result = context.eval(result, binaryExpr);
        if (supportTimeSeriesMetadataType(context)
            && dropMetricName
            && (result.schema().isOpen() || result.schema().labels().contains(LabelMatcher.NAME))) {
            Expression metadata = metadataWithout(result, Set.of(LabelMatcher.NAME));
            LogicalPlan resultPlan = result.plan();
            if (Objects.equals(metadata, result.metadata()) == false) {
                resultPlan = new Eval(source(), resultPlan, List.of(new Alias(source(), MetadataAttribute.TIMESERIES, metadata)));
            }
            var remaining = new TreeSet<>(result.schema().labels());
            remaining.remove(LabelMatcher.NAME);
            result = result.with(resultPlan, project(result.schema(), remaining), result.value());
            if (result.kind().afterInitialAggregation) {
                result = PromqlPlan.checkUnique(source(), result);
            }
        }
        return result;
    }

    /**
     * Translates a vector-matched join operator into an {@link InnerJoin}: each operand becomes an independent series
     * pipeline, joined on shared {@code step} + label keys, and the result value is computed on the joined rows.
     * The operands compile against the labels the join requires, like any other schema push-down: a required label
     * comes back as a concrete column wherever the operand can carry it, and a label the operand dropped stays
     * absent and null-fills at the join.
     * <p>
     * The block has three rules: how the sides are ordered (which operand probes, which builds and is re-identified), how
     * the fields are placed (each side's match key alongside step, the build side's value and {@code group_x} labels
     * carried across, every result label bound to the operand carrying it or to null) and what is projected (the build
     * side down to its join fields, the result down to value, step and the schema's labels). The result exposes its step
     * column under the enclosing translation's step identity, and a label the match dropped null-fills rather than leaking
     * from an operand.
     */
    public IntermediateResult translateJoinOperator(TranslationContext context) {
        TranslationSchema schema = union(finite(namedLabels(output())), finite(context.required().labels()));
        if (hasTsMetadata(output())) {
            // Keep result identity separate from the join keys. Parent aggregations may request additional
            // metadata projections, but the full result identity must survive until those aggregations consume it.
            TranslationSchema identity = union(open(), context.required());
            schema = union(schema, dropMetricName ? subtract(identity, Set.of(LabelMatcher.NAME)) : identity);
        }
        if (supportTimeSeriesMetadataType(context) && schema.isOpen()) {
            schema = union(finite(schema.labels()), open());
        }
        TranslationSchema childSchema = union(schema, finite(match.groupingLabels()));
        if (match.filter() == VectorMatch.Filter.ON) {
            childSchema = union(childSchema, finite(match.filterLabels()));
        } else if (match.filter() == VectorMatch.Filter.IGNORING) {
            childSchema = union(childSchema, open(match.filterLabels()));
        }

        // Preserve each operand's series identity, including on the build side. Grouping only by the match keys
        // could hide duplicates that the join must reject.
        boolean leftHasTsMetadata = hasTsMetadata(left().output());
        boolean rightHasTsMetadata = hasTsMetadata(right().output());
        IntermediateResult left = translateOperand(context, left(), childSchema, leftHasTsMetadata);
        IntermediateResult right = translateOperand(context, right(), childSchema, rightHasTsMetadata);
        // The probe carries the many side; the build must be unique per match key.
        // Without a group modifier both sides must be unique and the result retains the left labels.
        boolean probeRight = match.grouping() == Joining.RIGHT;
        IntermediateResult probe = probeRight ? right : left;
        IntermediateResult build = new PlanRefresher(context.cmd().valueColumnName()).apply(probeRight ? left : right);
        Expression leftValue = probeRight ? build.value() : probe.value();
        Expression rightValue = probeRight ? probe.value() : build.value();

        if (supportTimeSeriesMetadataType(context) && schema.isOpen()) {
            // Promoted columns remain part of the many side's identity; never lose them when retaining its residual metadata.
            var labels = new TreeSet<>(schema.labels());
            labels.addAll(probe.schema().labels());
            if (dropMetricName) {
                labels.remove(LabelMatcher.NAME);
            }
            if (match.grouping() == Joining.NONE && match.filter() == VectorMatch.Filter.IGNORING) {
                labels.removeAll(match.filterLabels());
            }
            schema = union(finite(labels), open());
        }
        LogicalPlan join = emitInnerJoin(context, probe, build);
        BoundOutput output = bindOutput(context, schema, output(), probe, build);
        IntermediateResult result = projectFinal(context, schema, leftValue, rightValue, probe.step(), join, output);
        // Consume the complete join before exposing rows, including when comparison filtering or LIMIT
        // would otherwise hide a later cardinality violation in an operand.
        return supportTimeSeriesMetadataType(context) ? PromqlPlan.checkUnique(source(), result) : result;
    }

    /** Named labels exclude the internal metadata identity, which is never itself a PromQL label. */
    static List<String> namedLabels(List<Attribute> attributes) {
        return PromqlLabels.labelNames(attributes.stream().filter(a -> PromqlLabels.isTsMetadata(a) == false).toList());
    }

    /** Finish each operand at its own series grain before matching; absent samples are not vector elements. */
    final IntermediateResult translateOperand(
        TranslationContext context,
        LogicalPlan operand,
        TranslationSchema required,
        boolean hasTsMetadata
    ) {
        IntermediateResult result = context.withRequired(hasTsMetadata ? union(required, open()) : required)
            .translateIntermediate(operand, new NameId(), new NameId());
        return result.with(
            new Filter(source(), result.plan(), new IsNotNull(source(), result.valueColumn())),
            result.schema(),
            result.value()
        );
    }

    /**
     * The operator's value computed on the joined rows, then the finished table: value and step exposed under this
     * frame's identities, null-fills defined, comparison filter mode applied, and everything else projected away.
     */
    private IntermediateResult projectFinal(
        TranslationContext context,
        TranslationSchema schema,
        Expression leftValue,
        Expression rightValue,
        Attribute step,
        LogicalPlan join,
        BoundOutput output
    ) {
        PromqlCommand cmd = context.cmd();
        Expression lhsExpr = new ToDouble(leftValue.source(), leftValue);
        Expression rhsExpr = new ToDouble(rightValue.source(), rightValue);
        Expression value = binaryOp().asFunction().create(source(), lhsExpr, rhsExpr, context.configuration());
        Expression filter = null;
        if (this instanceof VectorBinaryComparison comparison) {
            filter = comparison.filterMode() ? value : null;
            value = comparison.filterMode() ? (returnsScalar(left()) ? rhsExpr : lhsExpr) : new ToDouble(value.source(), value);
        }
        // Expose the step under the enclosing frame's step identity so enclosing translations (union branches, parent
        // aggregates) resolve it by id, not just by name.
        Alias stepAlias = new Alias(step.source(), step.name(), step, context.stepAttr().id());
        Alias valueAlias = new Alias(source(), cmd.valueColumnName(), value, new NameId());
        List<Alias> definitions = new ArrayList<>(List.of(valueAlias, stepAlias));
        definitions.addAll(defined(output.columns()));
        // Filter while both operand attributes are visible; the result alias can shadow the probe value.
        LogicalPlan plan = filter == null ? join : new Filter(source(), join, filter);
        plan = new Eval(cmd.source(), plan, definitions);
        List<NamedExpression> projected = new ArrayList<>(List.of(valueAlias.toAttribute(), stepAlias.toAttribute()));
        output.columns().forEach(column -> projected.add(column.toAttribute()));
        plan = new Project(cmd.source(), plan, projected);

        return new IntermediateResult(
            plan,
            schema,
            valueAlias.toAttribute(),
            stepAlias.toAttribute(),
            null,
            Kind.AFTER_INITIAL_AGGREGATE,
            output.labels()
        );
    }

    /** Join on step and the same ordered label keys on both sides. */
    private LogicalPlan emitInnerJoin(TranslationContext context, IntermediateResult probe, IntermediateResult build) {
        List<String> labelsToMatchOn = matchingLabels(probe, build);

        // Only the build value and explicitly copied labels are added to the probe's output.
        var addToProbeSideOutput = new ArrayList<Attribute>();
        addToProbeSideOutput.add(build.valueColumn());
        for (var name : match.groupingLabels()) {
            var field = build.label(name);
            if (field != null) {
                addToProbeSideOutput.add(field);
            }
        }

        PreparedJoinInput buildInput = prepareJoinInput(context, build, labelsToMatchOn);
        var buildProjection = new ArrayList<>(buildInput.keys());
        buildProjection.addAll(addToProbeSideOutput);

        PreparedJoinInput probeInput = prepareJoinInput(context, probe, labelsToMatchOn);

        // one-to-one join rejects multiple probe matched rows to a (always unique) build row.
        boolean shouldRejectsProbeDuplicates = match.grouping() == Joining.NONE;
        Source source = context.cmd().source();
        return new InnerJoin(
            source,
            probeInput.plan(),
            new Project(source, buildInput.plan(), buildProjection),
            probeInput.keys(),
            buildInput.keys(),
            addToProbeSideOutput,
            shouldRejectsProbeDuplicates,
            supportTimeSeriesMetadataType(context)
        );
    }

    /** The named part of the match key; residual metadata supplies the remaining labels. */
    final List<String> matchingLabels(IntermediateResult probe, IntermediateResult build) {
        var labelsToMatchOn = new TreeSet<String>();
        if (match.filter() == VectorMatch.Filter.ON) {
            labelsToMatchOn.addAll(match.filterLabels());
        } else {
            labelsToMatchOn.addAll(probe.schema().labels());
            labelsToMatchOn.addAll(build.schema().labels());
            labelsToMatchOn.removeAll(match.filterLabels());
            labelsToMatchOn.remove(LabelMatcher.NAME);
        }

        return List.copyOf(labelsToMatchOn);
    }

    /** Define absent labels as null, then form one match key alongside step; an empty key joins on step alone. */
    final PreparedJoinInput prepareJoinInput(TranslationContext context, IntermediateResult input, List<String> keyLabels) {
        if (supportTimeSeriesMetadataType(context)) {
            var excluded = new TreeSet<>(match.filterLabels());
            excluded.add(LabelMatcher.NAME);
            Expression metadata = match.filter() == VectorMatch.Filter.ON ? null : metadataWithout(input, excluded);
            var labels = new ArrayList<Expression>();
            for (String name : new TreeSet<>(keyLabels)) {
                Literal label = Literal.keyword(source(), name);
                Expression value = input.labelColumn(name);
                if (value == null) {
                    Attribute residual = input.metadata();
                    value = residual == null ? Literal.NULL : new TimeSeriesGet(source(), residual, label);
                }
                labels.add(label);
                labels.add(value);
            }
            Expression hash = new TimeSeriesHash(
                source(),
                metadata == null ? new Literal(source(), null, DataType.TIMESERIES_DATA_TYPE) : metadata,
                labels
            );
            Alias key = new Alias(source(), TemporaryNameGenerator.locallyUniqueTemporaryName("match_key"), hash);
            return new PreparedJoinInput(new Eval(source(), input.plan(), List.of(key)), List.of(input.step(), key.toAttribute()));
        }
        List<NamedExpression> key = new ArrayList<>();
        for (var name : keyLabels) {
            Attribute attribute = input.label(name);
            key.add(attribute != null ? attribute : TranslationContext.emitNullExpression(TranslationContext.stub(name)));
        }
        if (key.isEmpty()) {
            return new PreparedJoinInput(input.plan(), List.of(input.step()));
        }

        Source source = context.cmd().source();
        List<Alias> nullFills = defined(key);
        LogicalPlan plan = nullFills.isEmpty() ? input.plan() : new Eval(source, input.plan(), nullFills);
        Attribute packed = new ReferenceAttribute(source, null, PackDims.PACKED_FIELD_NAME, DataType.KEYWORD);
        plan = new PackDims(source, plan, key.stream().map(NamedExpression::toAttribute).toList(), packed);
        return new PreparedJoinInput(plan, List.of(input.step(), packed));
    }

    /** The join result's label columns: every schema label bound to the operand carrying it, or to null. */
    private BoundOutput bindOutput(
        TranslationContext context,
        TranslationSchema schema,
        List<Attribute> declared,
        IntermediateResult probe,
        IntermediateResult build
    ) {
        var output = new ArrayList<NamedExpression>();
        Map<String, Attribute> bindings = supportTimeSeriesMetadataType(context) ? new HashMap<>() : null;
        // Legacy joins require concrete label sets; only typed metadata reaches an open result.
        if (schema.isOpen()) {
            var excluded = new TreeSet<String>();
            if (dropMetricName) {
                excluded.add(LabelMatcher.NAME);
            }
            if (match.grouping() == Joining.NONE && match.filter() == VectorMatch.Filter.IGNORING) {
                excluded.addAll(match.filterLabels());
            }
            output.add(new Alias(source(), MetadataAttribute.TIMESERIES, metadataWithout(probe, excluded)));
        }
        for (var name : schema.labels()) {
            // A label the match semantics dropped (e.g. on(...) narrowing) may still be required by an enclosing
            // translation; it must come back null rather than leak through from an operand.
            var declaredAttr = bindings == null
                ? TranslationContext.find(declared, name)
                : declared.stream()
                    .filter(attribute -> PromqlLabels.isTsMetadata(attribute) == false && PromqlLabels.labelName(attribute).equals(name))
                    .findFirst()
                    .orElse(null);
            boolean removed = (dropMetricName && name.equals(LabelMatcher.NAME))
                || (declaredAttr == null && hasTsMetadata(declared) == false);
            if (removed && bindings == null) {
                output.add(TranslationContext.emitNullExpression(TranslationContext.stub(name)));
                continue;
            }
            Attribute attribute = removed ? null : (match.groupingLabels().contains(name) ? build.label(name) : probe.label(name));
            // Null-fill under the operator's own attribute when the carrying operand lacks the label, so the command
            // projection binds it by identity.
            NamedExpression column;
            if (attribute != null) {
                column = attribute;
            } else if (bindings != null) {
                column = new Alias(
                    source(),
                    TemporaryNameGenerator.locallyUniqueTemporaryName("label"),
                    new Literal(
                        source(),
                        null,
                        declaredAttr != null && declaredAttr.resolved() ? declaredAttr.dataType() : DataType.KEYWORD
                    )
                );
            } else {
                column = TranslationContext.emitNullExpression(declaredAttr != null ? declaredAttr : TranslationContext.stub(name));
            }
            output.add(column);
            if (bindings != null) {
                bindings.put(name, column.toAttribute());
            }
        }
        return new BoundOutput(output, bindings);
    }

    /** Build the residual identity for matching or output without removing labels already carried as columns. */
    private Expression metadataWithout(IntermediateResult input, Set<String> names) {
        var excluded = input.schema().labelsInMetadata(names);
        Attribute metadata = input.metadata();
        if (excluded.isEmpty()) {
            return metadata;
        }
        var labels = excluded.stream().map(name -> (Expression) Literal.keyword(source(), name)).toList();
        return new TimeSeriesUnset(source(), metadata, labels);
    }

    /** The columns among {@code columns} defined inline (aliases) rather than carried by the plan. */
    private static List<Alias> defined(List<? extends NamedExpression> columns) {
        return columns.stream().filter(Alias.class::isInstance).map(Alias.class::cast).toList();
    }

    private static Set<String> groupingNames(Aggregate aggregate) {
        Set<String> names = new HashSet<>();
        for (Expression grouping : aggregate.groupings()) {
            if (grouping instanceof NamedExpression named) {
                names.add(named.name());
            }
        }
        return names;
    }

    /** Fusion requires one aggregation over the same input and exactly the same grouping expressions. */
    private static boolean canMergeAggregates(LogicalPlan left, LogicalPlan right) {
        return left instanceof Aggregate leftAggregate
            && right instanceof Aggregate rightAggregate
            && leftAggregate.groupings().equals(rightAggregate.groupings())
            && leftAggregate.child().equals(rightAggregate.child())
            && leftAggregate.child().anyMatch(p -> p instanceof Aggregate || p instanceof Eval) == false;
    }

    /** Fold left and right aggregates into a single aggregate node. */
    private LogicalPlan mergeAggregateOperators(IntermediateResult left, IntermediateResult right) {
        var names = new TemporaryNameGenerator.Monotonic();
        var rightAgg = right.plan().collect(Aggregate.class).getFirst();
        Set<String> rightGroupingNames = groupingNames(rightAgg);

        var result = left.plan().transformDown(Aggregate.class, leftAgg -> {
            boolean groupingsCompatible = leftAgg.groupings().size() == rightAgg.groupings().size()
                && groupingNames(leftAgg).equals(rightGroupingNames);

            if (groupingsCompatible == false) {
                throw new VerificationException("binary operations between vectors with mismatched grouping keys are not yet supported");
            }

            var uniqueAggregates = new LinkedHashSet<Expression>();
            uniqueAggregates.addAll(withFilter(leftAgg.aggregates(), left.pendingFilter()));
            uniqueAggregates.addAll(withFilter(rightAgg.aggregates(), right.pendingFilter()));

            // Only the aggregate functions need fresh names: both operands define `value`. Grouping columns keep their
            // own names - the command projection finds a passthrough label (`labels.pod`) by its canonical name when the
            // analyzer bound the declared output to the bare attribute instead, and a renamed column would not map.
            var newAggregates = uniqueAggregates.stream().map(e -> (NamedExpression) e).map(e -> {
                if (e instanceof Alias a) {
                    return (NamedExpression) new Alias(a.source(), names.next(a.name()), a.child(), a.id());
                }
                return e;
            }).toList();

            return leftAgg.with(leftAgg.child(), leftAgg.groupings(), newAggregates);
        });

        var rightEvals = right.plan().collect(Eval.class);
        for (Eval eval : rightEvals.reversed()) {
            result = new Eval(eval.source(), result, eval.fields());
        }
        return result;
    }

}
