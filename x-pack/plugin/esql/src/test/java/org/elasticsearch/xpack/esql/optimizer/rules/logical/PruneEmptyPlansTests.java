/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.index.IndexProperties;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.plan.logical.Drain;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.logical.join.InnerJoin;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.promql.CheckSeriesUniqueness;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.unboundLogicalOptimizerContext;

/** Empty-result shortcuts must preserve eager validation and reach a stable plan without changing its schema. */
public class PruneEmptyPlansTests extends ESTestCase {
    private final Attribute step = new ReferenceAttribute(Source.EMPTY, "step", DataType.DATETIME);
    private final Attribute identity = new ReferenceAttribute(Source.EMPTY, "identity", DataType.KEYWORD);
    private final Attribute value = new ReferenceAttribute(Source.EMPTY, "value", DataType.DOUBLE);

    /** Both explicit and folded false predicates must consume an effectful child. */
    public void testFalseFiltersDrainEffectfulChildren() {
        for (LogicalPlan child : List.of(checked(), new Drain(Source.EMPTY, checked()), join(true))) {
            LogicalPlan pruned = new PruneFilters().apply(new Filter(Source.EMPTY, child, Literal.FALSE));
            assertDrainedChild(child, pruned);
            assertSame(pruned, new PruneFilters().apply(pruned));
        }
    }

    /** LIMIT 0 suppresses results, but cannot bypass duplicate-series validation. */
    public void testZeroLimitsDrainEffectfulChildren() {
        var context = unboundLogicalOptimizerContext(TransportVersion.current());
        var rule = new SkipQueryOnLimitZero();
        for (LogicalPlan child : List.of(checked(), new Drain(Source.EMPTY, checked()), join(true))) {
            var limit = new Limit(Source.EMPTY, new Literal(Source.EMPTY, 0, DataType.INTEGER), child);
            LogicalPlan pruned = rule.apply(limit, context);
            assertDrainedChild(child, pruned);
            assertSame(pruned, rule.apply(pruned, context));
        }
    }

    /** A discarded histogram's numeric null value must not allow its enclosing null filter to erase validations. */
    public void testFoldedNullFilterRetainsValidation() {
        var alias = new Alias(Source.EMPTY, "result", new Literal(Source.EMPTY, null, DataType.DOUBLE));
        var eval = new Eval(Source.EMPTY, new Drain(Source.EMPTY, checked()), List.of(alias));
        var filter = new Filter(Source.EMPTY, eval, new IsNotNull(Source.EMPTY, alias.toAttribute()));
        var optimizer = new LogicalPlanOptimizer(unboundLogicalOptimizerContext(TransportVersion.current()));
        LogicalPlan optimized = optimizer.optimize(filter);
        assertEquals(filter.output(), optimized.output());
        assertTrue(optimized.anyMatch(Drain.class::isInstance));
        assertTrue(optimized.anyMatch(CheckSeriesUniqueness.class::isInstance));
        assertTrue(optimized.anyMatch(EsRelation.class::isInstance));
        assertEquals(optimized, optimizer.optimize(optimized));
    }

    /** Discarding sorted results must keep validations without leaving an unsupported unbounded sort. */
    public void testDiscardedSortRetainsValidation() {
        var sorted = sorted(checked());
        for (LogicalPlan plan : List.of(
            new Limit(Source.EMPTY, new Literal(Source.EMPTY, 0, DataType.INTEGER), sorted),
            new Filter(Source.EMPTY, sorted, Literal.FALSE)
        )) {
            var optimizer = new LogicalPlanOptimizer(unboundLogicalOptimizerContext(TransportVersion.current()));
            LogicalPlan optimized = optimizer.optimize(plan);
            assertEquals(plan.output(), optimized.output());
            assertTrue(optimized.anyMatch(Drain.class::isInstance));
            assertTrue(optimized.anyMatch(CheckSeriesUniqueness.class::isInstance));
            assertTrue(optimized.anyMatch(EsRelation.class::isInstance));
            assertFalse(optimized.anyMatch(OrderBy.class::isInstance));
            assertEquals(optimized, optimizer.optimize(optimized));
        }
    }

    /** Projection does not make input ordering observable when every resulting row is discarded. */
    public void testDrainPrunesSortThroughProjection() {
        var check = checked();
        var project = new Project(Source.EMPTY, sorted(check), List.of(value));
        var drain = new Drain(Source.EMPTY, project);
        LogicalPlan optimized = new PruneRedundantOrderBy().apply(drain);
        assertEquals(drain.output(), optimized.output());
        assertSame(check, as(as(optimized, Drain.class).child(), Project.class).child());
    }

    /** A bounded selection can change which rows reach validation, so its order cannot be removed by an enclosing drain. */
    public void testDrainPreservesBoundedSelection() {
        var limit = new Literal(Source.EMPTY, 1, DataType.INTEGER);
        var sorted = sorted(checked());
        for (LogicalPlan selection : List.of(
            new Limit(Source.EMPTY, limit, sorted),
            new TopN(Source.EMPTY, checked(), sorted.order(), limit, false)
        )) {
            var drain = new Drain(Source.EMPTY, selection);
            assertSame(drain, new PruneRedundantOrderBy().apply(drain));
        }
    }

    /** Optional pruning must not replace computed output attributes with references that the child does not expose. */
    public void testDifferentOutputSchemaIsRetained() {
        var alias = new Alias(Source.EMPTY, "renamed", value);
        var eval = new Eval(Source.EMPTY, checked(), List.of(alias));
        assertSame(eval, PruneEmptyPlans.skipPlan(eval));
        var project = new Project(Source.EMPTY, eval, List.of(alias.toAttribute()));
        assertSame(project, PruneEmptyPlans.skipPlan(project));
    }

    /** Empty projections and drains remain unchanged on repeated application instead of accumulating drain wrappers. */
    public void testEmptyOutputReachesFixedPoint() {
        var rule = new PruneEmptyPlans();
        var project = new Project(Source.EMPTY, checked(), List.of());
        var drain = new Drain(Source.EMPTY, project);
        for (LogicalPlan plan : List.of(project, drain)) {
            assertSame(plan, rule.apply(plan));
            assertSame(plan, rule.apply(rule.apply(plan)));
        }
    }

    /** A guard can validate row cardinality using constant keys even when it exposes no columns. */
    public void testGuardItselfCannotBePruned() {
        var child = new Project(Source.EMPTY, relation(), List.of());
        var check = new CheckSeriesUniqueness(
            Source.EMPTY,
            child,
            new Literal(Source.EMPTY, 0L, DataType.DATETIME),
            Literal.keyword(Source.EMPTY, "identity")
        );
        assertTrue(check.output().isEmpty());
        assertSame(check, PruneEmptyPlans.skipPlan(check));
    }

    /** Ordinary queries retain their existing empty-plan shortcut. */
    public void testOrdinaryChildrenStillPrune() {
        var context = unboundLogicalOptimizerContext(TransportVersion.current());
        for (LogicalPlan child : List.of(relation(), join(false))) {
            var filter = new Filter(Source.EMPTY, child, Literal.FALSE);
            LogicalPlan filtered = new PruneFilters().apply(filter);
            assertEquals(child.output(), filtered.output());
            assertTrue(as(filtered, LocalRelation.class).hasEmptySupplier());
            var limit = new Limit(Source.EMPTY, new Literal(Source.EMPTY, 0, DataType.INTEGER), child);
            LogicalPlan limited = new SkipQueryOnLimitZero().apply(limit, context);
            assertEquals(child.output(), limited.output());
            assertTrue(as(limited, LocalRelation.class).hasEmptySupplier());
        }
    }

    private void assertDrainedChild(LogicalPlan child, LogicalPlan pruned) {
        assertEquals(child.output(), pruned.output());
        if (child instanceof Drain) {
            assertSame(child, pruned);
        } else {
            assertSame(child, as(pruned, Drain.class).child());
        }
    }

    private CheckSeriesUniqueness checked() {
        return new CheckSeriesUniqueness(Source.EMPTY, relation(), step, identity);
    }

    private OrderBy sorted(LogicalPlan child) {
        return new OrderBy(
            Source.EMPTY,
            child,
            List.of(new Order(Source.EMPTY, value, Order.OrderDirection.ASC, Order.NullsPosition.LAST))
        );
    }

    private InnerJoin join(boolean eager) {
        var rightIdentity = new ReferenceAttribute(Source.EMPTY, "right_identity", DataType.KEYWORD);
        var right = new Project(
            Source.EMPTY,
            relation(),
            List.of(new Alias(Source.EMPTY, rightIdentity.name(), identity, rightIdentity.id()))
        );
        return new InnerJoin(Source.EMPTY, relation(), right, List.of(identity), List.of(rightIdentity), List.of(), true, eager);
    }

    private EsRelation relation() {
        return new EsRelation(
            Source.EMPTY,
            "metrics",
            IndexMode.STANDARD,
            Map.of("", List.of("metrics")),
            Map.of("", List.of("metrics")),
            Map.of("metrics", new IndexProperties(IndexMode.STANDARD, 1)),
            List.of(step, identity, value)
        );
    }
}
