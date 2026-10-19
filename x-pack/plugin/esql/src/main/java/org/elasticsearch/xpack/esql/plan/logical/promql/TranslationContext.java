/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.analysis.AnalyzerContext;
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.expression.TimeSeriesMetadataAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.CountOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.First;
import org.elasticsearch.xpack.esql.expression.function.aggregate.TimeSeriesAggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Values;
import org.elasticsearch.xpack.esql.expression.function.grouping.TStep;
import org.elasticsearch.xpack.esql.expression.function.grouping.TimeSeriesWithout;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.Case;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDatetime;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDouble;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Sub;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.esql.parser.promql.PromqlLogicalPlanBuilder;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.PackDims;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.TopNBy;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.UnpackDims;
import org.elasticsearch.xpack.esql.plan.logical.join.AbstractSubqueryJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.InnerJoin;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContext.IntermediateResult.Kind;
import org.elasticsearch.xpack.esql.plan.logical.promql.operator.VectorBinaryComparison;
import org.elasticsearch.xpack.esql.plan.logical.promql.operator.VectorBinarySet;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.LiteralSelector;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.Selector;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.elasticsearch.xpack.esql.expression.predicate.Predicates.combineAndNullable;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationSchema.finite;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationSchema.intersect;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationSchema.open;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationSchema.project;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationSchema.union;

/**
 * Shared state and assembly helpers for one PromQL translation. Nodes own their lowering, while this context
 * preserves the existing schema bookkeeping, aggregation boundaries, and command finalization.
 */
public record TranslationContext(
    PromqlCommand cmd,
    AnalyzerContext analyzer,
    /* Alias for the step bucket expression used in all aggregation groupings. May be null for empty indices. */
    Alias stepBucketAlias,
    /* The columns the result subtree MUST expose. */
    TranslationSchema required,
    /* The current evaluation time (default: @timestamp). */
    Expression time,
    /* Shared by every branch so materialized evaluation grids cannot each spend the full planning budget. */
    FoldContext foldContext
) {

    // Sentinel bounds for open-ended range queries (PROMQL step=X without explicit start/end): TStep requires explicit bounds,
    // so pass the widest representable range. EPOCH/MAX_MILLIS_BEFORE_9999 avoid time boundary handling in the engine.
    private static final Instant EPOCH_MIN = Instant.EPOCH;
    private static final Instant EPOCH_MAX = Instant.ofEpochMilli(DateUtils.MAX_MILLIS_BEFORE_9999);

    /** The command exposes every label of the result series: the full open label space. */
    public TranslationContext(PromqlCommand cmd, AnalyzerContext analyzer) {
        this(cmd, analyzer, null, open(), null, analyzer.configuration().newFoldContext());
    }

    /** Send label requirements down to a child without changing the enclosing branch's timing or aggregation state. */
    public TranslationContext withRequired(TranslationSchema childRequired) {
        return new TranslationContext(
            cmd,
            analyzer,
            stepBucketAlias,
            supportTimeSeriesMetadataType(this) && childRequired.isOpen() ? union(finite(childRequired.labels()), open()) : childRequired,
            time,
            foldContext
        );
    }

    /**
     * Dispatches to the node that owns the translation. The source relation is the leaf of the produced ES|QL subtree;
     * requirements travel down the PromQL tree and intermediate results travel back up.
     */
    public IntermediateResult translate(LogicalPlan node) {
        if (node instanceof PromqlPlan promql) {
            return promql.translateWithRequirements(this);
        }
        throw new QlIllegalArgumentException("Unsupported PromQL plan node: {}", node);
    }

    /**
     * Keep verification and translation on the same metadata representation for the entire command.
     * Typed metadata requires the feature flag and support on every participating node.
     */
    public static boolean supportTimeSeriesMetadataType(TranslationContext context) {
        PromqlCommand command = context.cmd();
        AnalyzerContext analyzer = context.analyzer();
        return command != null
            && analyzer.flags().promqlFunctionalTimeSeries()
            && TimeSeriesMetadataAttribute.supportTimeSeriesMetadataType(analyzer.minimumVersion());
    }

    public Configuration configuration() {
        return analyzer.configuration();
    }

    public Attribute stepAttr() {
        return stepBucketAlias != null ? stepBucketAlias.toAttribute() : cmd.stepAttribute();
    }

    /** Translates one merge branch with its own step bucket and evaluation time. */
    public IntermediateResult translateIntermediate(LogicalPlan branch, NameId stepId, NameId valueId) {
        Expression branchTime = cmd.collectEvaluationTimestampForBranch(evaluationClock(branch));
        Alias step = canCreateStepBucket() ? emitStepBucketExpression(stepId, branchTime) : null;
        var run = new TranslationContext(cmd, analyzer, step, required, branchTime, foldContext);
        return run.translateIntermediate(branch, valueId);
    }

    private LogicalPlan evaluationClock(LogicalPlan branch) {
        // Source-free operands share the command's clock. An offset on the source vector must shift its constant
        // operands too; source-backed operands keep their own selector clocks.
        return supportTimeSeriesMetadataType(this)
            && branch.anyMatch(p -> p instanceof Selector && p instanceof LiteralSelector == false) == false ? cmd.promqlPlan() : branch;
    }

    /**
     * Materialize a constant once per source-derived evaluation step, independently of selector label predicates.
     * Open-ended queries have no synthetic time range: without source rows there are no steps to populate. Set
     * operands may use different offsets, so their distinct clocks contribute steps to one deduplicated domain.
     */
    IntermediateResult sourceSteps(LogicalPlan clock, Expression value) {
        if (cmd.child() instanceof EsRelation relation && relation.concreteQualifiedIndices().isEmpty()) {
            var empty = new LocalRelation(cmd.source(), List.of(cmd.valueAttribute(), cmd.stepAttribute()), EmptyLocalSupplier.EMPTY);
            return new IntermediateResult(empty, TranslationSchema.EMPTY, value, cmd.stepAttribute(), null, Kind.EMPTY, Map.of());
        }
        var clocks = new LinkedHashMap<Duration, LogicalPlan>();
        for (Selector selector : clock.collect(Selector.class)) {
            if (selector instanceof LiteralSelector == false) {
                clocks.putIfAbsent(cmd.collectFirstOffsetForBranch(selector), selector);
            }
        }
        if (clocks.size() <= 1) {
            return sourceStepsForClock(clock, value);
        }
        var branches = new ArrayList<LogicalPlan>();
        for (LogicalPlan selectorClock : clocks.values()) {
            IntermediateResult steps = sourceStepsForClock(selectorClock, value);
            branches.add(new Project(cmd.source(), steps.plan(), List.of(steps.step())));
        }
        // A common step can occur on several clocks. Group before supplying scalar defaults, otherwise equal-priority
        // rows would survive KeepMinimum and turn one scalar sample into several.
        var step = new ReferenceAttribute(cmd.source(), cmd.stepColumnName(), DataType.DATETIME);
        LogicalPlan union = new UnionAll(cmd.source(), branches, List.of(step));
        Alias constant = new Alias(value.source(), cmd.valueColumnName(), new Values(value.source(), value));
        LogicalPlan plan = new Aggregate(cmd.source(), union, groupings(step, List.of()), aggregates(constant, step, List.of()));
        return new IntermediateResult(
            plan,
            TranslationSchema.EMPTY,
            constant.toAttribute(),
            step,
            null,
            Kind.AFTER_INITIAL_AGGREGATE,
            Map.of()
        );
    }

    private IntermediateResult sourceStepsForClock(LogicalPlan clock, Expression value) {
        Expression evaluationTime = cmd.collectEvaluationTimestampForBranch(clock);
        Alias step = emitStepBucketExpression(new NameId(), evaluationTime);
        var run = new TranslationContext(cmd, analyzer, step, TranslationSchema.EMPTY, evaluationTime, foldContext);
        LogicalPlan source = run.pushDownEvaluationTimestampFilter(cmd.child(), clock);
        var input = new IntermediateResult(
            source,
            TranslationSchema.EMPTY,
            value,
            step.toAttribute(),
            null,
            Kind.BEFORE_INITIAL_AGGREGATE,
            Map.of()
        );
        return run.collapse(input, TranslationSchema.EMPTY, value);
    }

    /** Finishes the command, including independently translated union branches and the declared output projection. */
    public LogicalPlan translateFinal() {
        if (supportTimeSeriesMetadataType(this)
            && cmd.child() instanceof EsRelation relation
            && relation.concreteQualifiedIndices().isEmpty()
            && cmd.promqlPlan().anyMatch(p -> p instanceof Selector && p instanceof LiteralSelector == false)
            && cmd.promqlPlan().anyMatch(p -> p instanceof VectorBinarySet || p instanceof ScalarConversionFunction) == false) {
            // The supported source-backed operators cannot produce series without input. Keep the declared
            // result columns, but do not build dictionary expressions for a source that has no metadata.
            return new LocalRelation(cmd.source(), cmd.output(), EmptyLocalSupplier.EMPTY);
        }
        // `or` is the only set operator that adds rows (more series), requiring a top-level multi-branch `UnionAll` that
        // cannot compose as a single-value sub-expression.
        // PromQL `or` is left-associative, so flatten the top-level chain into independent branches.
        var branches = new ArrayList<LogicalPlan>();
        if (supportTimeSeriesMetadataType(this)) {
            branches.add(cmd.promqlPlan());
        } else {
            flattenUnion(cmd.promqlPlan(), branches);
        }

        if (branches.size() == 1) {
            IntermediateResult intermediateResult;
            if (supportTimeSeriesMetadataType(this)) {
                intermediateResult = finishCommandValue(translateIntermediate(cmd.promqlPlan(), cmd.stepId(), new NameId()));
            } else {
                intermediateResult = translateIntermediate(cmd.promqlPlan(), cmd.stepId(), cmd.valueId());
            }
            Attribute declared = find(cmd.output(), MetadataAttribute.TIMESERIES);
            LogicalPlan plan = emitTimeSeriesAlias(intermediateResult, declared != null ? declared.id() : new NameId());
            return doTranslateFinal(plan, intermediateResult.kind().constant, intermediateResult);
        }
        // Compile every branch as its own module (own step/value ids, own shifted evaluation timestamp), then link.
        var intermediateResultPlan = doTranslateUnion(
            branches.stream().map(b -> translateIntermediate(b, new NameId(), new NameId())).toList()
        );
        return doTranslateFinal(intermediateResultPlan, false, null);
    }

    /* Shared by every `final` translation root */
    private LogicalPlan doTranslateFinal(LogicalPlan plan, boolean localRelation, @Nullable IntermediateResult result) {
        plan = emitNullsFilter(cmd.source(), emitFinalProjection(plan, result), cmd.valueAttribute());
        return localRelation ? plan : emitByStepFilter(plan);
    }

    /** Internal tables may carry histograms; the command's public value column still requires a numeric result. */
    private IntermediateResult finishCommandValue(IntermediateResult input) {
        Alias value = emitValueDoubleCastExpression(input.value(), cmd.valueId());
        LogicalPlan plan = new Eval(cmd.source(), input.plan(), List.of(value));
        return input.with(plan, input.schema(), value.toAttribute());
    }

    /**
     * A finished table exposes its metadata under the canonical {@code _timeseries} name. Metadata projections travel under
     * their derived names so nodes can tell them apart; the one surviving at a root is whatever the enclosing
     * regroups left, and the command declares it as {@code _timeseries}.
     */
    private LogicalPlan emitTimeSeriesAlias(IntermediateResult table, NameId id) {
        if (supportTimeSeriesMetadataType(this) && table.schema().isOpen()) {
            return cmd.renderTimeSeries(table, id);
        }
        Set<String> skip = table.schema().finestSkip();
        if (skip == null || skip.isEmpty()) {
            return table.plan();
        }
        Attribute metadata = metadata(table, skip);
        assert metadata != null : "invariant: metadata column " + skip + " must be carried by the finished table";
        return new Eval(cmd.source(), table.plan(), List.of(new Alias(cmd.source(), MetadataAttribute.TIMESERIES, metadata, id)));
    }

    /**
     * Union combinator over independently translated tabular results.
     * {@link UnionAll} aligns columns by name and null-fills the missing ones, then
     * {@link TopNBy} keeps single row per {@code (step, labelset)} group ordered by incoming IR order.
     */
    private LogicalPlan doTranslateUnion(List<IntermediateResult> intermediateResults) {
        // Already validated against max_branch_count_per_merge by PromqlCommand.verify.
        int maxBranches = analyzer.maxBranchCountPerMerge();
        assert intermediateResults.size() <= maxBranches
            : "invariant: merge branch count ["
                + intermediateResults.size()
                + "] must be <= the max_branch_count_per_merge limit ["
                + maxBranches
                + "]";

        var source = cmd.source();
        var branchPlans = new ArrayList<LogicalPlan>(intermediateResults.size());
        for (int i = 0; i < intermediateResults.size(); i++) {
            // Drop null-valued rows per branch so an absent left side does not shadow a present right side.
            var ir = intermediateResults.get(i);
            LogicalPlan branchPlan = emitNullsFilter(source, emitTimeSeriesAlias(ir, new NameId()), ir.valueColumn());
            var branchTagExpression = new Alias(source, cmd.branchColumnName(), new Literal(source, i, DataType.INTEGER));
            LogicalPlan tagged = new Eval(source, branchPlan, List.of(branchTagExpression));
            // Each branch executes as an independent sub plan whose result pages cross an exchange, and the
            // consumer assumes their layout matches output() exactly. An Eval below (e.g. the value double-cast)
            // can name-shadow an existing column: the shadowed attribute leaves output() but its channel stays
            // in the page. An explicit projection pins the page layout to the branch output (see #158164).
            branchPlans.add(new Project(source, tagged, tagged.output()));
        }

        // The attribute ids chosen here are preserved by name when the analyzer later recomputes the UnionAll output,
        // so the groupings below remain valid. The command coda projects the synthetic branch tag away.
        List<Attribute> unionOutput = VectorBinarySet.unionOutputByName(branchPlans);
        var union = new UnionAll(source, branchPlans, unionOutput);

        // Left-preferring dedup: group by every column except the value and the branch tag, keep the lowest branch.
        var groupings = new ArrayList<Expression>();
        Attribute branchAttr = null;
        for (Attribute attr : unionOutput) {
            if (attr.name().equals(cmd.branchColumnName())) {
                branchAttr = attr;
            } else if (attr.name().equals(cmd.valueColumnName()) == false) {
                groupings.add(attr);
            }
        }
        var order = new Order(source, branchAttr, Order.OrderDirection.ASC, Order.NullsPosition.LAST);
        return new TopNBy(source, union, List.of(order), new Literal(source, 1, DataType.INTEGER), groupings);
    }

    /**
     * Translates independent query fragment into intermediate result (IR).
     * Think of IR as table
     */
    private IntermediateResult translateIntermediate(LogicalPlan branch, NameId valueId) {
        IntermediateResult ir = inlineConstantSteps(translate(branch));
        LogicalPlan clock = evaluationClock(branch);

        var plan = ir.plan();
        var value = ir.value();
        // A vector match self-filters each operand's own source with that operand's own @timestamp; a combined outer
        // source-time filter would push one operand's @timestamp across both sources.
        Expression timeFilter = plan.anyMatch(p -> p instanceof InnerJoin || p instanceof AbstractSubqueryJoin || p instanceof UnionAll)
            ? null
            : emitBySrcTimeFilter(clock);
        var filter = combineAndNullable(Arrays.asList(ir.pendingFilter(), timeFilter));
        if (filter != null) {
            plan = pushDownSrcTimestampFilter(plan, filter);
        }

        if (ir.kind().constant == false) {
            // TimeSeriesAggregate always applies because InstantSelectors adds implicit last_over_time().
            // TODO: with metric references without last_over_time, a plain Aggregate could do (#141501 discussion).
            if (ir.kind().afterInitialAggregation == false) {
                IntermediateResult collapsed = collapse(ir.with(plan, ir.schema(), value), ir.schema(), value);
                plan = collapsed.plan();
                value = collapsed.value();
            }
            if (branch instanceof VectorBinaryComparison comparison && comparison.filterMode()) {
                if (supportTimeSeriesMetadataType(this) == false
                    && comparison.canTranslateAsAggregate(this)
                    && comparison.right() instanceof LiteralSelector literal) {
                    // Filter-mode comparison (metric > x): keep the left operand's value, filter rows by the comparison.
                    // A vector-matched comparison already applied its filter inside the join translation.
                    ToDouble right = new ToDouble(comparison.right().source(), literal.literal());
                    var condition = comparison.op().asFunction().create(comparison.source(), value, right, configuration());
                    plan = new Filter(comparison.source(), plan, condition);
                }
            }
        }

        // Relabeling and set operations preserve histogram samples for an enclosing histogram function to consume.
        Alias valueAlias = supportTimeSeriesMetadataType(this) && value.resolved() && value.dataType().isHistogram()
            ? new Alias(cmd.source(), cmd.valueColumnName(), value, valueId)
            : emitValueDoubleCastExpression(value, valueId);
        plan = new Eval(cmd.source(), plan, List.of(valueAlias));
        if (ir.kind().constant == false) {
            plan = pushDownEvaluationTimestampFilter(plan, clock);
        }

        Kind kind = ir.kind().constant ? ir.kind() : Kind.AFTER_INITIAL_AGGREGATE;
        return new IntermediateResult(
            plan,
            ir.schema(),
            valueAlias.toAttribute(),
            ir.step(),
            null,
            kind,
            ir.retainedBindings(plan, ir.schema())
        );
    }

    /** Folds a branch whose value depends on nothing but the step column into a compile-time step/value relation. */
    IntermediateResult inlineConstantSteps(IntermediateResult result) {
        Attribute stepAttr = cmd.stepAttribute();
        if (result.kind().constant || result.schema().equals(TranslationSchema.EMPTY) == false) {
            return result;
        }
        if (cmd.start().value() == null) {
            return supportTimeSeriesMetadataType(this)
                && result.kind().afterInitialAggregation == false
                && result.pendingFilter() == null
                && result.value().foldable() ? sourceSteps(cmd.promqlPlan(), result.value()) : result;
        }
        if (result.value().references().stream().allMatch(ref -> ref.semanticEquals(stepAttr)) == false) {
            return result;
        }
        var plan = PromqlLogicalPlanBuilder.buildLocalRelation(cmd, foldContext);
        var step = plan.output().getFirst();
        var value = result.value().transformUp(Attribute.class, attr -> attr.semanticEquals(stepAttr) ? step : attr);
        return new IntermediateResult(
            plan,
            result.schema(),
            value,
            step,
            result.pendingFilter(),
            Kind.CONSTANT,
            result.retainedBindings(plan, result.schema())
        );
    }

    /**
     * Expressions compose lazily up the tree until they cross an aggregation boundary: once the plan below is
     * aggregated, the expression must materialize as the value column (an Eval) so parents reference it by attribute.
     */
    public IntermediateResult eval(IntermediateResult t, Expression value) {
        if (t.kind().afterInitialAggregation == false) {
            return t.with(t.plan(), t.schema(), value);
        }
        Alias alias = new Alias(value.source(), cmd.valueColumnName(), value);
        return t.with(new Eval(cmd.source(), t.plan(), List.of(alias)), t.schema(), alias.toAttribute());
    }

    /**
     * The table a {@code without} regroup exposes: the child columns surviving the dropped labels. Under a metadata
     * column the labels are derived columns, so only those the enclosing translation asks for are carried; a finite
     * child keeps every remaining label because they are its label set.
     */
    public TranslationSchema regroupWithout(TranslationSchema child, List<String> keys) {
        if (supportTimeSeriesMetadataType(this)) {
            var remaining = new LinkedHashSet<>(child.labels());
            remaining.removeAll(keys);
            return project(child, remaining);
        }
        TranslationSchema schema = intersect(child, keys);
        assert child.isOpen() == false || schema.isOpen()
            : "invariant: required [" + required + "] must declare a metadata column excluding " + keys + ", got " + child;
        return schema.isOpen() ? project(schema, required.labels()) : schema;
    }

    /**
     * The initial aggregate: a raw table collapsed to one row per step and schema column by the innermost
     * {@link TimeSeriesAggregate}, {@code function} applied in it. Passing the table's own value collapses it as is.
     */
    public IntermediateResult collapse(IntermediateResult input, TranslationSchema schema, Expression function) {
        assert input.kind().afterInitialAggregation == false : "invariant: a collapse takes a raw table";
        if (supportTimeSeriesMetadataType(this)
            && schema.isOpen()
            && function.resolved()
            && (function instanceof AggregateFunction == false || function instanceof TimeSeriesAggregateFunction)) {
            return collapseSeries(input, schema, function);
        }
        Alias value = new Alias(function.source(), cmd.valueColumnName(), function);
        return table(emitCollapse(input, schema, value, List.of()), input, schema, value);
    }

    private IntermediateResult collapseSeries(IntermediateResult input, TranslationSchema schema, Expression function) {
        List<EsRelation> sources = input.plan().collect(EsRelation.class);
        if (sources.size() != 1) {
            throw new IllegalStateException("A raw series table must have exactly one time-series source");
        }
        EsRelation source = sources.getFirst();
        Attribute seriesId = source.output()
            .stream()
            .filter(attribute -> attribute.dataType() == DataType.TSID_DATA_TYPE)
            .findFirst()
            .orElseGet(() -> new MetadataAttribute(source.source(), MetadataAttribute.TSID_FIELD, DataType.TSID_DATA_TYPE, false));
        if (source.output().contains(seriesId) == false) {
            var attributes = new ArrayList<>(source.output());
            attributes.add(seriesId);
            EsRelation withSeriesId = source.withAttributes(attributes);
            LogicalPlan plan = input.plan().transformUp(EsRelation.class, relation -> relation == source ? withSeriesId : relation);
            input = input.with(plan, input.schema(), input.value());
        }
        // Keep physical series separate until canonical identity is checked: empty and absent dimension values
        // produce different _tsids but the same PromQL labels, even when their sample values are equal.
        Expression sample = function;
        if (function.dataType().isHistogram()) {
            // Phase one emits one sample per (_tsid, step); retaining both keys makes FIRST single-input.
            sample = new First(function.source(), function, new Literal(function.source(), 0, DataType.INTEGER));
        }
        Alias value = new Alias(function.source(), cmd.valueColumnName(), sample);
        IntermediateResult result = table(emitCollapse(input, schema, value, List.of(seriesId)), input, schema, value);
        result = PromqlPlan.checkUnique(function.source(), result);
        List<Attribute> output = result.plan()
            .output()
            .stream()
            .filter(attribute -> attribute.id().equals(seriesId.id()) == false)
            .toList();
        return result.with(new Project(function.source(), result.plan(), output), schema, result.value());
    }

    /**
     * An aggregate over a collapsed table: regrouped by {@code schema} with {@code function} as the value. The regroup
     * packs its dimensions first when the schema is open or the operator asks for it ({@code packed}).
     */
    public IntermediateResult regroup(IntermediateResult input, TranslationSchema schema, boolean packed, Expression function) {
        assert input.kind().afterInitialAggregation : "invariant: a regroup takes a collapsed table";
        Alias value = new Alias(function.source(), cmd.valueColumnName(), function);
        return table(emitRegroup(input, schema, value, schema.isOpen() || packed), input, schema, value);
    }

    private static IntermediateResult table(LogicalPlan plan, IntermediateResult input, TranslationSchema schema, Alias value) {
        return new IntermediateResult(
            plan,
            schema,
            value.toAttribute(),
            input.step(),
            input.pendingFilter(),
            Kind.AFTER_INITIAL_AGGREGATE,
            input.retainedBindings(plan, schema)
        );
    }

    /**
     * The innermost aggregate owns the physical {@code _timeseries} grouping and materializes every metadata column in
     * the schema over that column's own skip set.
     */
    private LogicalPlan emitCollapse(IntermediateResult input, TranslationSchema schema, Alias value, List<Attribute> internalGroupings) {
        Source source = cmd.promqlPlan().source();
        LogicalPlan plan = input.plan();
        boolean groupsBySeries = schema.isOpen() || schema.labels().isEmpty() == false;
        Expression agg = value.child();
        // TranslateTimeSeriesAggregate splits this node into two phases, replacing inner TimeSeriesAggregateFunctions
        // (e.g. LastOverTime) with references to phase-1 results; the phase-2 expression must remain a valid
        // AggregateFunction inside the Aggregate node:
        // Sum(LastOverTime(m)) -> Sum(ref) -- Sum survives, no wrap needed
        // LastOverTime(m) -> ref -- bare ref, needs Values(ref)
        // Mul(LastOverTime(m), 8) -> Mul(ref, 8) -- not an agg, needs Values(Mul(ref,8))
        // Guarded by groupsBySeries because without any series grouping (e.g. constants like vector(5))
        // TranslateTimeSeriesAggregate passes Literals straight to phase 1.
        boolean wrapWithValues = (agg instanceof AggregateFunction == false) || (agg instanceof TimeSeriesAggregateFunction);
        if (groupsBySeries && wrapWithValues) {
            value = value.replaceChild(new Values(agg.source(), agg));
        }

        // Canonical residual metadata and the promoted labels together identify a series. Group the typed metadata
        // once; legacy schemas still need a source projection for each requested exclusion set.
        var groupKeys = new ArrayList<NamedExpression>(internalGroupings);
        var outKeys = new ArrayList<NamedExpression>(internalGroupings);
        if (supportTimeSeriesMetadataType(this)) {
            if (schema.isOpen()) {
                groupKeys.add(input.metadata());
                outKeys.add(input.metadata());
            }
        } else {
            for (Set<String> skip : coarse(schema.skips())) {
                List<Expression> excluded = skip.stream().<Expression>map(label -> {
                    Attribute resolved = find(plan.output(), label);
                    return resolved != null ? resolved : stub(label);
                }).toList();
                Alias metadata = new Alias(source, mapOpen(skip), new TimeSeriesWithout(source, excluded));
                groupKeys.add(metadata);
                outKeys.add(metadata.toAttribute());
            }
        }
        for (String label : schema.labels()) {
            Attribute carrier = input.labelBindings() != null ? input.labelColumn(label) : find(plan.output(), label);
            if (carrier != null) {
                groupKeys.add(carrier);
                outKeys.add(carrier);
            }
        }

        return new TimeSeriesAggregate(
            source,
            plan,
            groupings(stepBucketAlias, groupKeys),
            aggregates(value, input.step(), outKeys),
            null,
            time,
            TimeSeriesAggregate.Origin.PROMQL_COMMAND
        );
    }

    /**
     * Regroups an already-aggregated table. Every regroup first resolves its physical schema and null-fills missing
     * grouping columns. A packed regroup additionally packs dimensions before aggregation to prevent multi-valued
     * dimensions from splitting rows and double-counting, then unpacks them afterwards.
     */
    private LogicalPlan emitRegroup(IntermediateResult input, TranslationSchema schema, Alias value, boolean requiresPacking) {
        Source source = cmd.source();
        Attribute step = input.step();
        LogicalPlan plan = input.plan();
        if (value.child() instanceof AggregateFunction == false) {
            value = value.replaceChild(new Values(value.child().source(), value.child()));
        }
        List<Attribute> available = plan.output();

        var nulls = new ArrayList<Alias>();
        var keys = new ArrayList<Attribute>();
        if (supportTimeSeriesMetadataType(this)) {
            if (schema.isOpen()) {
                keys.add(input.metadata());
            }
        } else {
            for (Set<String> skip : coarse(schema.skips())) {
                Attribute carrier = metadata(input, skip);
                assert carrier != null : "invariant: metadata column " + skip + " must be carried by the child";
                keys.add(carrier);
            }
        }
        for (String label : schema.labels()) {
            Attribute carrier = input.labelBindings() != null ? input.labelColumn(label) : find(available, label);
            if (carrier == null) {
                // a declared label the child lacks is absent from every series: grouped under null, like Prometheus
                nulls.add(emitNullExpression(stub(label)));
                carrier = nulls.getLast().toAttribute();
            }
            keys.add(carrier);
        }

        if (nulls.isEmpty() == false) {
            plan = new Eval(source, plan, nulls);
        }

        if (requiresPacking == false || (supportTimeSeriesMetadataType(this) && schema.isOpen())) {
            return new Aggregate(source, plan, groupings(step, keys), aggregates(value, step, keys));
        }
        // TranslateTimeSeriesAggregate unpacks the inner TSA's dimensions and this regroup re-packs them.
        if (keys.isEmpty()) {
            return new Aggregate(source, plan, groupings(step, List.of()), aggregates(value, step, List.of()));
        }
        Attribute packedAttribute = PackDims.newPackedAttribute(source);
        PackDims packDims = new PackDims(source, plan, keys, packedAttribute);
        Alias packedGrouping = PackDims.newPackedGrouping(source, packedAttribute);
        Aggregate agg = new Aggregate(
            source,
            packDims,
            groupings(step, List.of(packedGrouping)),
            aggregates(value, step, List.of(packedGrouping.toAttribute()))
        );
        List<Attribute> unpackedDims = keys.stream()
            .<Attribute>map(
                dim -> new ReferenceAttribute(dim.source(), null, dim.name(), dim.dataType().noText(), Nullability.TRUE, dim.id(), false)
            )
            .toList();
        UnpackDims unpackDims = new UnpackDims(source, agg, packedGrouping.toAttribute(), unpackedDims);
        List<NamedExpression> projections = new ArrayList<>(List.of(value.toAttribute(), step));
        projections.addAll(unpackedDims);
        return new Project(source, unpackDims, projections);
    }

    /** Projects the plan to the command's declared output, re-aliasing columns that match by name but not by id. */
    private LogicalPlan emitFinalProjection(LogicalPlan plan, @Nullable IntermediateResult result) {
        var lookupMap = new HashMap<String, Attribute>();
        for (var attr : plan.output()) {
            lookupMap.put(attr.name(), attr);
        }
        // Under a passthrough mapping the plan carries the concrete field (`labels.job`) while the command declares
        // the label alone, so fall back to the canonical name.
        for (var attr : plan.output()) {
            String label = PromqlLabels.labelName(attr);
            if ((result == null || result.labelBindings() == null) && attr instanceof FieldAttribute == false) {
                // Legacy UnpackDims retains physical names such as labels.job in ordinary references.
                // Typed translation binds labels explicitly and keeps derived names literal.
                label = PromqlLabels.labelName(attr.name());
            }
            lookupMap.putIfAbsent(label, attr);
        }
        var projected = new ArrayList<>(cmd.output());
        var evals = new ArrayList<Alias>();
        for (int i = 0; i < projected.size(); i++) {
            var attr = projected.get(i);
            Attribute lookupAttr;
            if (result != null && result.labelBindings() != null && i >= 2) {
                if (result.schema().isOpen() && PromqlLabels.isTsMetadata(attr)) {
                    lookupAttr = plan.output().stream().filter(candidate -> candidate.id().equals(attr.id())).findFirst().orElseThrow();
                } else {
                    lookupAttr = result.labelColumn(PromqlLabels.labelName(attr));
                }
            } else {
                lookupAttr = lookupMap.get(attr.name());
            }
            if (lookupAttr != null && lookupAttr.semanticEquals(attr) == false) {
                var alias = new Alias(lookupAttr.source(), attr.name(), lookupAttr, attr.id());
                evals.add(alias);
                projected.set(i, alias.toAttribute());
            }
        }
        if (evals.isEmpty() == false) {
            plan = new Eval(cmd.source(), plan, evals);
        }
        return new Project(cmd.source(), plan, projected);
    }

    /** Keeps only steps within the query range; steps are anchored at {@code start} and offset-independent. */
    private LogicalPlan emitByStepFilter(LogicalPlan plan) {
        var source = cmd.source();
        var step = cmd.stepAttribute();
        var start = cmd.start();
        var end = cmd.end();
        var lo = new GreaterThanOrEqual(source, step, start.value() != null ? start : Literal.dateTime(source, EPOCH_MIN));
        var hi = new LessThanOrEqual(source, step, end.value() != null ? end : Literal.dateTime(source, EPOCH_MAX));
        return new Filter(source, plan, new And(source, lo, hi));
    }

    /**
     * The source-time pushdown predicate. Expressed over the <b>raw</b> source timestamp (not the offset-shifted
     * evaluation timestamp) so it can push down to the index; the branch offset is instead folded into the bounds.
     * Expressing it over the shifted timestamp while also adjusting the bounds would apply the offset twice.
     */
    private Expression emitBySrcTimeFilter(LogicalPlan branch) {
        if (cmd.start().value() == null || cmd.end().value() == null) {
            return null;
        }
        var source = cmd.source();
        var offset = cmd.collectFirstOffsetForBranch(branch);
        var timestamp = cmd.timestamp();
        var window = cmd.sourceFilterWindow();
        var lo = new Sub(source, cmd.start(), Literal.timeDuration(source, window.plus(offset)), configuration());
        var hi = new Sub(source, cmd.end(), Literal.timeDuration(source, offset), configuration());
        return new And(source, new GreaterThanOrEqual(source, timestamp, lo), new LessThanOrEqual(source, timestamp, hi));
    }

    /** Adds an Eval on top of the source relation materializing the evaluation timestamp (@timestamp + offset). */
    public LogicalPlan pushDownEvaluationTimestampFilter(LogicalPlan plan, LogicalPlan branch) {
        if (time instanceof ReferenceAttribute ref && cmd.timestampColumnName().equals(ref.name())) {
            Expression base = cmd.timestamp();
            if (base.dataType() == DataType.DATE_NANOS) {
                base = new ToDatetime(base.source(), base, configuration());
            }
            var offset = cmd.collectFirstOffsetForBranch(branch);
            var shifted = offset.isZero() ? base : new Add(cmd.source(), base, Literal.timeDuration(cmd.source(), offset), configuration());
            var time = new Alias(cmd.source(), cmd.timestampColumnName(), shifted, ref.id());
            return plan.transformUp(node -> node == cmd.child(), node -> new Eval(cmd.source(), node, List.of(time)));
        }
        return plan;
    }

    /** Pushes the label filter down to the EsRelation, combining with an existing relation filter. */
    private LogicalPlan pushDownSrcTimestampFilter(LogicalPlan plan, Expression filterCondition) {
        return plan.transformUp(LogicalPlan.class, p -> {
            if (p instanceof Filter f && f.child() instanceof EsRelation) {
                return new Filter(f.source(), f.child(), new And(f.source(), f.condition(), filterCondition));
            } else if (p instanceof EsRelation) {
                return new Filter(cmd.source(), p, filterCondition);
            }
            return p;
        });
    }

    /** The value column definition: the translateIntermediate's value expression, cast to double unless it provably is one. */
    private Alias emitValueDoubleCastExpression(Expression valueExpr, NameId valueId) {
        if ((valueExpr instanceof Attribute == false && valueExpr.resolved() && valueExpr.dataType() == DataType.DOUBLE) == false) {
            valueExpr = new ToDouble(cmd.source(), valueExpr);
        }
        return new Alias(cmd.source(), cmd.valueColumnName(), valueExpr, valueId);
    }

    /**
     * The {@code step} bucket for a branch: the {@link TStep} grouping key shared across all aggregation groupings,
     * derived from the (possibly offset-shifted) evaluation timestamp - so an {@code offset} shifts which samples
     * fall into each fixed output bucket without moving the buckets. {@code stepId} names the synthetic column.
     */
    private Alias emitStepBucketExpression(NameId stepId, Expression time) {
        Expression size;
        Expression start;
        Expression end;
        if (cmd.isInstantQuery()) {
            size = Literal.timeDuration(cmd.source(), cmd.resolveInstantQueryWindow());
            start = new Sub(cmd.source(), cmd.start(), size, configuration());
            end = cmd.end();
        } else {
            size = cmd.resolveTimeBucketSize();
            start = cmd.start().value() != null ? cmd.start() : Literal.dateTime(cmd.source(), EPOCH_MIN);
            end = cmd.end().value() != null ? cmd.end() : Literal.dateTime(cmd.source(), EPOCH_MAX);
        }
        var tstep = new TStep(size.source(), size, start, end, time, configuration());
        return new Alias(tstep.source(), cmd.stepColumnName(), tstep, stepId);
    }

    private boolean canCreateStepBucket() {
        if (cmd.timestamp() == null || cmd.timestamp().resolved() == false) {
            return cmd.isRangeQuery() == false || cmd.buckets() == null || cmd.buckets().value() == null;
        }
        return true;
    }

    private static List<Expression> groupings(Expression step, List<? extends NamedExpression> keys) {
        var groupings = new ArrayList<Expression>(keys.size() + 1);
        groupings.add(step);
        groupings.addAll(keys);
        return groupings;
    }

    private static List<NamedExpression> aggregates(NamedExpression value, Attribute step, List<? extends NamedExpression> keys) {
        var aggregates = new ArrayList<NamedExpression>(keys.size() + 2);
        aggregates.add(value);
        aggregates.add(step);
        aggregates.addAll(keys);
        return aggregates;
    }

    /** Flattens a left-associative top-level {@code or} chain into branches; branch 0 has the highest precedence. */
    private static void flattenUnion(LogicalPlan node, List<LogicalPlan> branches) {
        if (node instanceof VectorBinarySet setOp && setOp.op() == VectorBinarySet.SetOp.UNION) {
            flattenUnion(setOp.left(), branches);
            flattenUnion(setOp.right(), branches);
        } else {
            branches.add(node);
        }
    }

    /** PromQL drops series with missing data: filter out rows whose value is null (null label columns are valid). */
    private static LogicalPlan emitNullsFilter(Source source, LogicalPlan plan, Attribute value) {
        return new Filter(source, plan, new IsNotNull(value.source(), value));
    }

    // -- core --

    /**
     * The single value flowing through the compiler: a table - an ESQL plan together with its defined columns. The
     * {@link TranslationSchema} names the label columns and the plan carries them; value and step are the two
     * columns every table has. Every AST node translates to one and the stitching operations (joins, unions,
     * regroups, the command coda) compose them by their declared columns. Mid-descent the value is a (possibly not
     * yet materialized) expression parents compose into larger expressions; a finished table's value is a defined
     * column ({@link #valueColumn()}).
     */
    public record IntermediateResult(
        /* Output ESQL plan: the source relation (cmd.child()) with this node's operators stacked on top. */
        LogicalPlan plan,
        /* Logical labels this subtree exposes; physical carriers are bound separately below. */
        TranslationSchema schema,
        /* This node's numeric value: an expression mid-descent, a defined column once aggregated. */
        Expression value,
        /* The step column. */
        Attribute step,
        /* Label matcher predicate; flows up until pushed to the relation or folded into an aggregate filter. */
        Expression pendingFilter,
        /* The translator tracks what it built instead of inspecting the plan. */
        Kind kind,
        /* Explicit logical-label bindings on the typed path; null preserves the legacy name-based adapter. */
        @Nullable Map<String, Attribute> labelBindings
    ) {
        public IntermediateResult {
            if (labelBindings != null) {
                labelBindings = Map.copyOf(labelBindings);
                if (labelBindings.keySet().equals(schema.labels()) == false) {
                    throw new IllegalArgumentException("Label bindings must match the translation schema");
                }
            }
        }

        /** The legacy adapter keeps its existing name-based label lookup. */
        public IntermediateResult(
            LogicalPlan plan,
            TranslationSchema schema,
            Expression value,
            Attribute step,
            Expression pendingFilter,
            Kind kind
        ) {
            this(plan, schema, value, step, pendingFilter, kind, null);
        }

        /** The lifecycle of an intermediate result. Constant steps and known-empty input are both source-independent. */
        public enum Kind {
            BEFORE_INITIAL_AGGREGATE(false, false),
            AFTER_INITIAL_AGGREGATE(true, false),
            CONSTANT(true, true),
            EMPTY(true, true);

            /** A local relation needs no source filtering or aggregation. */
            public final boolean constant;
            /** Value expressions above this boundary must materialize before being consumed. */
            public final boolean afterInitialAggregation;

            Kind(boolean afterInitialAggregation, boolean constant) {
                this.afterInitialAggregation = afterInitialAggregation;
                this.constant = constant;
            }
        }

        /** A raw input whose value may still contain per-series aggregate expressions. */
        public IntermediateResult(LogicalPlan plan, TranslationSchema schema, Expression value, Attribute step) {
            this(plan, schema, value, step, null, Kind.BEFORE_INITIAL_AGGREGATE);
        }

        /** A raw input carrying a selector predicate until source filtering or aggregate assembly consumes it. */
        public IntermediateResult(LogicalPlan plan, TranslationSchema schema, Expression value, Attribute step, Expression selectorFilter) {
            this(plan, schema, value, step, selectorFilter, Kind.BEFORE_INITIAL_AGGREGATE);
        }

        /** This table rebuilt around a new plan, schema and value, keeping its other properties. */
        public IntermediateResult with(LogicalPlan plan, TranslationSchema schema, Expression value) {
            return with(plan, schema, value, retainedBindings(plan, schema));
        }

        /** Rebuilds the table when a translator defines or replaces label columns explicitly. */
        public IntermediateResult with(
            LogicalPlan plan,
            TranslationSchema schema,
            Expression value,
            @Nullable Map<String, Attribute> labelBindings
        ) {
            return new IntermediateResult(plan, schema, value, step, pendingFilter, kind, labelBindings);
        }

        /** Carries surviving bindings by identity; generated physical names never define logical labels. */
        @Nullable
        public Map<String, Attribute> retainedBindings(LogicalPlan plan, TranslationSchema schema) {
            if (labelBindings == null) {
                return null;
            }
            var available = new HashMap<NameId, Attribute>();
            plan.output().forEach(attribute -> available.put(attribute.id(), attribute));
            var retained = new HashMap<String, Attribute>();
            for (String name : schema.labels()) {
                Attribute previous = labelBindings.get(name);
                Attribute current = previous == null ? null : available.get(previous.id());
                if (current == null) {
                    throw new IllegalStateException("Missing column for promoted label [" + name + "]");
                }
                retained.put(name, current);
            }
            return retained;
        }

        /** The value as a defined column; only valid on a finished table. */
        public Attribute valueColumn() {
            return (Attribute) value;
        }

        /** The attribute carrying a label in this table's plan, or null when the table lacks it. */
        public Attribute label(String name) {
            return labelBindings != null ? labelBindings.get(name) : find(plan.output(), name);
        }

        /** The functional path carries one typed dictionary, bound through the same schema as its named columns. */
        public Attribute metadata() {
            if (schema.isOpen() == false) {
                return null;
            }
            var columns = plan.output().stream().filter(a -> a.resolved() && a.dataType() == DataType.TIMESERIES_DATA_TYPE).toList();
            if (columns.size() != 1) {
                throw new IllegalStateException("Expected one time-series metadata column, found [" + columns.size() + "]");
            }
            return columns.getFirst();
        }

        /** Resolve a current label column, ignoring temporary columns outside the schema. */
        public Attribute labelColumn(String name) {
            if (schema.labels().contains(name) == false) {
                return null;
            }
            Attribute column = label(name);
            if (column == null) {
                throw new IllegalStateException("Missing column for promoted label [" + name + "]");
            }
            return column;
        }
    }

    /** A null-valued column under the attribute's own name and id, typed like the attribute (keyword when unresolved). */
    public static Alias emitNullExpression(Attribute attribute) {
        var nullLiteral = new Literal(attribute.source(), null, attribute.resolved() ? attribute.dataType() : DataType.KEYWORD);
        return new Alias(attribute.source(), attribute.name(), nullLiteral, attribute.id());
    }

    // -- helpers --

    /** Legacy metadata projections retain the names expected by the keyword-based WITHOUT translation. */
    public static String mapOpen(Set<String> skip) {
        return MetadataAttribute.TIMESERIES + (skip.isEmpty() ? "" : "$" + String.join("$", new TreeSet<>(skip)));
    }

    /** Resolve the current typed metadata, or the legacy projection for these exclusions. */
    public Attribute metadata(IntermediateResult table, Set<String> skip) {
        return supportTimeSeriesMetadataType(this) ? table.metadata() : find(table.plan().output(), mapOpen(skip));
    }

    /** A missing label's reference, subsequently defined as null by its consumer. */
    public static Attribute stub(String name) {
        return new ReferenceAttribute(Source.EMPTY, null, name, DataType.KEYWORD);
    }

    /** The skip sets of a schema ordered finest first: the metadata projection fixing the grain leads, coarser variants follow. */
    public static List<Set<String>> coarse(Set<Set<String>> skips) {
        return skips.stream().sorted(Comparator.comparingInt(Set::size)).toList();
    }

    /** Finds a canonical label, preferring its backing field to a same-named bare reference. */
    public static Attribute find(List<Attribute> attributes, String label) {
        Attribute exactMatch = null;
        for (Attribute attribute : attributes) {
            if (attribute.name().equals(label)) {
                exactMatch = attribute;
            }
            String canonical = attribute instanceof FieldAttribute
                ? PromqlLabels.labelName(attribute)
                : PromqlLabels.labelName(attribute.name());
            if (canonical.equals(label) && attribute.name().equals(label) == false) {
                return attribute;
            }
        }
        return exactMatch;
    }

    /**
     * A count is at least 1 for an element: {@code count} counts a group's series, {@code count_over_time} a series'
     * samples in the window, and a group or series with none is no element. Maps a count of 0 to null so the element
     * drops like any other without a value.
     */
    public static Expression zeroCountAsNull(Expression count) {
        Expression empty = new Equals(count.source(), count, new Literal(count.source(), 0L, DataType.LONG));
        return new Case(count.source(), empty, List.of(Literal.NULL, count));
    }

    /** {@link #zeroCountAsNull} for every {@code count_over_time} of a per-series value. */
    public static Expression emptySampleCountAsNull(Expression value) {
        return value.transformUp(CountOverTime.class, TranslationContext::zeroCountAsNull);
    }
}
