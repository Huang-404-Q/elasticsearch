/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql.operator;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesGet;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesUnset;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.TemporaryNameGenerator;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.KeepMinimum;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.PackDims;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.join.AntiJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.SemiJoin;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlLabels;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlPlan;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContext;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContext.IntermediateResult;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContext.IntermediateResult.Kind;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationSchema;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.elasticsearch.xpack.esql.plan.logical.promql.PromqlLabels.hasTsMetadata;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContext.supportTimeSeriesMetadataType;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationSchema.finite;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationSchema.open;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationSchema.union;

public final class VectorBinarySet extends VectorBinaryOperator {

    public enum SetOp implements BinaryOp {
        INTERSECT("and"),
        SUBTRACT("unless"),
        UNION("or");

        private final String keyword;

        SetOp(String keyword) {
            this.keyword = keyword;
        }

        /** The PromQL keyword for this operator ({@code and}/{@code unless}/{@code or}), as used in error messages. */
        public String keyword() {
            return keyword;
        }

        @Override
        public ScalarFunctionFactory asFunction() {
            throw new UnsupportedOperationException("not yet implemented");
        }
    }

    private final SetOp op;

    public VectorBinarySet(Source source, LogicalPlan left, LogicalPlan right, VectorMatch match, SetOp op) {
        // Set operators preserve the metric name (__name__); unlike arithmetic/comparison operators they do not drop it.
        super(source, left, right, match, false, op);
        this.op = op;
    }

    public SetOp op() {
        return op;
    }

    /**
     * Intersection and subtraction preserve the complete left identity. Union can return rows from either
     * operand, so it exposes both label sets. Conflicting dimension types use their common textual representation.
     */
    @Override
    public List<Attribute> output() {
        if (op != SetOp.UNION) {
            return left().output();
        }
        // A relabelled branch may turn a native dimension into text. Declare that common type before enclosing
        // ES|QL expressions bind to the result; changing only the translated union would leave their types stale.
        var labels = new LinkedHashMap<String, Attribute>();
        for (LogicalPlan operand : List.of(left(), right())) {
            for (Attribute attribute : operand.output()) {
                labels.merge(PromqlLabels.labelName(attribute), attribute, VectorBinarySet::commonLabelType);
            }
        }
        Map<String, DataType> types = labelTypes();
        return labels.values().stream().map(attribute -> {
            if (PromqlLabels.isTsMetadata(attribute) == false && types.get(PromqlLabels.labelName(attribute)) == DataType.KEYWORD) {
                return asTextLabel(attribute);
            }
            return attribute;
        }).toList();
    }

    @Override
    public Map<String, DataType> labelTypes() {
        if (op != SetOp.UNION) {
            return PromqlPlan.labelTypes(left());
        }
        var types = new HashMap<>(PromqlPlan.labelTypes(left()));
        PromqlPlan.labelTypes(right()).forEach((name, type) -> types.merge(name, type, (first, next) -> {
            return first == next ? first : DataType.KEYWORD;
        }));
        return Map.copyOf(types);
    }

    private static Attribute commonLabelType(Attribute first, Attribute next) {
        // A literal label named _timeseries remains in the dictionary when the union also has runtime labels.
        // The command's one _timeseries output must describe that dictionary, regardless of operand order.
        if (PromqlLabels.isTsMetadata(first)) {
            return first;
        }
        if (PromqlLabels.isTsMetadata(next)) {
            return next;
        }
        if (first.resolved() == false || next.resolved() == false || first.dataType() == next.dataType()) {
            return first;
        }
        return asTextLabel(first);
    }

    private static Attribute asTextLabel(Attribute attribute) {
        if (attribute.resolved() == false || attribute.dataType() == DataType.KEYWORD) {
            return attribute;
        }
        return new ReferenceAttribute(
            attribute.source(),
            null,
            attribute.name(),
            DataType.KEYWORD,
            attribute.nullable(),
            attribute.id(),
            attribute.synthetic()
        );
    }

    @Override
    public IntermediateResult translate(TranslationContext context) {
        if (supportTimeSeriesMetadataType(context) == false) {
            // Legacy top-level unions are flattened by TranslationContext before reaching this method.
            return super.translate(context);
        }
        TranslationSchema required = union(context.required(), finite(namedLabels(output())));
        if (match().filter() == VectorMatch.Filter.ON) {
            required = union(required, finite(match().filterLabels()));
        }
        IntermediateResult left = translateOperand(context, left(), required, hasTsMetadata(left().output()));
        IntermediateResult right = translateOperand(context, right(), required, hasTsMetadata(right().output()));
        List<String> keys = matchingLabels(left, right);
        if (op != SetOp.UNION) {
            LogicalPlan plan = membership(context, left, right, keys, op == SetOp.SUBTRACT);
            return finishLeft(context, left, plan);
        }

        return translateUnion(context, left, right, keys);
    }

    /** Membership joins deduplicate their build keys and retain each probe row once, including many-to-many matches. */
    private LogicalPlan membership(
        TranslationContext context,
        IntermediateResult probe,
        IntermediateResult build,
        List<String> keys,
        boolean exclude
    ) {
        PreparedJoinInput probeInput = prepareJoinInput(context, probe, keys);
        PreparedJoinInput buildInput = prepareJoinInput(context, new PlanRefresher(context.cmd().valueColumnName()).apply(build), keys);
        LogicalPlan buildKeys = new Project(source(), buildInput.plan(), buildInput.keys());
        return exclude
            ? new AntiJoin(source(), probeInput.plan(), buildKeys, probeInput.keys(), buildInput.keys(), true)
            : new SemiJoin(source(), probeInput.plan(), buildKeys, probeInput.keys(), buildInput.keys(), true);
    }

    /** Membership changes presence only; neither matching modifiers nor right-side labels change the output identity. */
    private IntermediateResult finishLeft(TranslationContext context, IntermediateResult left, LogicalPlan plan) {
        Alias step = new Alias(source(), context.cmd().stepColumnName(), left.step(), context.stepAttr().id());
        List<Alias> aliases = new ArrayList<>(List.of(step));
        List<NamedExpression> output = new ArrayList<>(left.plan().output());
        output.removeIf(attribute -> attribute.toAttribute().semanticEquals(left.step()));
        output.add(step.toAttribute());
        TranslationSchema schema = left.schema();
        if (hasTsMetadata(output()) && schema.isOpen() == false) {
            // An empty source has no dictionary column, but the empty result still needs its declared schema.
            Alias metadata = new Alias(source(), MetadataAttribute.TIMESERIES, new Literal(source(), null, DataType.TIMESERIES_DATA_TYPE));
            aliases.add(metadata);
            output.add(metadata.toAttribute());
            schema = union(schema, open());
        }
        plan = new Project(source(), new Eval(source(), plan, aliases), output);
        return new IntermediateResult(
            plan,
            schema,
            left.value(),
            step.toAttribute(),
            null,
            Kind.AFTER_INITIAL_AGGREGATE,
            left.retainedBindings(plan, schema)
        );
    }

    /** Align series columns without reducing the many rows that can share a set-matching key. */
    private IntermediateResult translateUnion(
        TranslationContext context,
        IntermediateResult left,
        IntermediateResult right,
        List<String> keyLabels
    ) {
        var labels = new TreeSet<>(left.schema().labels());
        labels.addAll(right.schema().labels());
        labels.addAll(namedLabels(output()));
        boolean metadata = hasTsMetadata(output()) || left.schema().isOpen() || right.schema().isOpen();
        TranslationSchema schema = metadata ? union(finite(labels), open()) : finite(labels);
        var columns = new HashMap<String, Attribute>();
        for (String label : labels) {
            Attribute leftType = left.labelColumn(label);
            Attribute type = leftType;
            Attribute rightType = right.labelColumn(label);
            if (type == null) {
                type = rightType;
            } else if (rightType != null) {
                type = commonLabelType(type, rightType);
            }
            // An unpromoted runtime label is text. Preserve a native type only when the AST declares that contract.
            if ((leftType == null && left.schema().isOpen()) || (rightType == null && right.schema().isOpen())) {
                type = new ReferenceAttribute(source(), label, DataType.KEYWORD);
            }
            Attribute declared = output().stream()
                .filter(attribute -> PromqlLabels.isTsMetadata(attribute) == false && PromqlLabels.labelName(attribute).equals(label))
                .findFirst()
                .orElse(null);
            if (declared != null) {
                type = declared;
            }
            columns.put(
                label,
                new ReferenceAttribute(
                    source(),
                    TemporaryNameGenerator.locallyUniqueTemporaryName("label"),
                    type != null ? type.dataType() : DataType.KEYWORD
                )
            );
        }
        String keyName = TemporaryNameGenerator.locallyUniqueTemporaryName("union_key");
        String priorityName = TemporaryNameGenerator.locallyUniqueTemporaryName("union_priority");
        LogicalPlan leftPlan = unionBranch(context, left, columns, metadata, keyLabels, keyName, priorityName, 0);
        LogicalPlan rightPlan = unionBranch(context, right, columns, metadata, keyLabels, keyName, priorityName, 1);
        // A union output is not any one branch's value. Fresh identities keep a branch-local constant
        // from being propagated above the merge and replacing values supplied by the other branch.
        List<Attribute> unionOutput = Expressions.toReferenceAttributesPreservingIds(
            unionOutputByName(List.of(leftPlan, rightPlan)),
            List.of()
        );
        LogicalPlan plan = new UnionAll(source(), List.of(leftPlan, rightPlan), unionOutput);
        plan = new KeepMinimum(
            source(),
            plan,
            TranslationContext.find(unionOutput, keyName),
            TranslationContext.find(unionOutput, priorityName)
        );
        List<Attribute> output = unionOutput.stream()
            .filter(attribute -> attribute.name().equals(keyName) == false && attribute.name().equals(priorityName) == false)
            .toList();
        plan = new Project(source(), plan, output);
        Attribute value = TranslationContext.find(output, context.cmd().valueColumnName());
        Attribute step = TranslationContext.find(output, context.cmd().stepColumnName());
        var bindings = new HashMap<String, Attribute>();
        columns.forEach((name, column) -> bindings.put(name, TranslationContext.find(output, column.name())));
        return finishLeft(context, new IntermediateResult(plan, schema, value, step, null, Kind.AFTER_INITIAL_AGGREGATE, bindings), plan);
    }

    /** Align each branch's promoted columns and residual metadata to the union's one shared schema. */
    private LogicalPlan unionBranch(
        TranslationContext context,
        IntermediateResult input,
        Map<String, Attribute> columns,
        boolean metadata,
        List<String> keyLabels,
        String keyName,
        String priorityName,
        int priority
    ) {
        PreparedJoinInput keyed = prepareJoinInput(context, input, keyLabels);
        // PackDims produces opaque bytes, which may be carried through a TopN as payload.
        Attribute key = new ReferenceAttribute(source(), keyName, DataType.SOURCE);
        LogicalPlan plan = new PackDims(source(), keyed.plan(), keyed.keys(), key);
        var aliases = new ArrayList<Alias>();
        aliases.add(new Alias(source(), context.cmd().valueColumnName(), input.value()));
        aliases.add(new Alias(source(), context.cmd().stepColumnName(), input.step()));
        aliases.add(new Alias(source(), priorityName, new Literal(source(), priority, DataType.INTEGER)));
        Attribute residual = input.metadata();
        for (String label : new TreeSet<>(columns.keySet())) {
            Attribute value = input.labelColumn(label);
            Attribute column = columns.get(label);
            Expression expression = value != null ? value
                : residual != null ? new TimeSeriesGet(source(), residual, Literal.keyword(source(), label))
                : new Literal(source(), null, column.dataType());
            if (expression.dataType() != column.dataType()) {
                expression = EsqlDataTypeConverter.converterFunctionFactory(column.dataType())
                    .apply(source(), expression, context.configuration());
            }
            aliases.add(new Alias(source(), column.name(), expression));
        }
        if (metadata) {
            Expression value = residual;
            if (value == null) {
                value = new TimeSeriesUnset(source(), new Literal(source(), null, DataType.TIMESERIES_DATA_TYPE), List.of());
            } else {
                List<Expression> promoted = new TreeSet<>(columns.keySet()).stream()
                    .map(label -> (Expression) Literal.keyword(source(), label))
                    .toList();
                value = new TimeSeriesUnset(source(), value, promoted);
            }
            aliases.add(new Alias(source(), MetadataAttribute.TIMESERIES, value));
        }
        plan = new Eval(source(), plan, aliases);
        var projected = new ArrayList<Attribute>(aliases.stream().map(Alias::toAttribute).toList());
        projected.add(key);
        return new Project(source(), plan, projected);
    }

    /**
     * Computes the combined output of the given plans as the union of their output attributes, deduplicated by
     * name with the first occurrence winning. This is the output schema of a set operator (see {@link #output()})
     * and is also used to compute the combined output of a flattened top-level {@code or} chain during translation.
     */
    public static List<Attribute> unionOutputByName(List<? extends LogicalPlan> plans) {
        List<Attribute> result = new ArrayList<>();
        Set<String> names = new HashSet<>();
        for (LogicalPlan plan : plans) {
            for (Attribute attr : plan.output()) {
                if (names.add(attr.name())) {
                    result.add(attr);
                }
            }
        }
        return result;
    }

    @Override
    public VectorBinarySet replaceChildren(LogicalPlan newLeft, LogicalPlan newRight) {
        return new VectorBinarySet(source(), newLeft, newRight, match(), op());
    }

    @Override
    protected NodeInfo<VectorBinarySet> info() {
        return NodeInfo.create(this, VectorBinarySet::new, left(), right(), match(), op());
    }
}
