/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.join;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.index.IndexProperties;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.plan.logical.Drain;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.promql.CheckSeriesUniqueness;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.unboundLogicalOptimizerContext;

public class EagerSubqueryJoinTests extends ESTestCase {
    private final Attribute leftStep = new ReferenceAttribute(Source.EMPTY, "step", DataType.DATETIME);
    private final Attribute leftKey = new ReferenceAttribute(Source.EMPTY, "left_key", DataType.KEYWORD);
    private final Attribute rightStep = new ReferenceAttribute(Source.EMPTY, "step", DataType.DATETIME);
    private final Attribute rightKey = new ReferenceAttribute(Source.EMPTY, "right_key", DataType.KEYWORD);

    public void testEmptyBuildDrainsEagerSemiJoinProbe() {
        for (boolean compositeKey : List.of(false, true)) {
            var join = join(false, true, checked(leftStep, leftKey), relation(rightStep, rightKey), compositeKey);
            var empty = new LocalRelation(Source.EMPTY, join.right().output(), EmptyLocalSupplier.EMPTY);
            LogicalPlan inlined = AbstractSubqueryJoin.inlineData(join, empty, 0, TestBlockFactory.getNonBreakingInstance(), null);
            assertSame(join.left(), as(inlined, Drain.class).child());
            assertEquals(join.output(), inlined.output());
        }
    }

    public void testDefaultEmptyBuildShortcutIsUnchanged() {
        var join = join(false, false, checked(leftStep, leftKey), relation(rightStep, rightKey), true);
        var empty = new LocalRelation(Source.EMPTY, join.right().output(), EmptyLocalSupplier.EMPTY);
        LogicalPlan inlined = AbstractSubqueryJoin.inlineData(join, empty, 0, TestBlockFactory.getNonBreakingInstance(), null);
        assertTrue(as(inlined, LocalRelation.class).hasEmptySupplier());
        assertEquals(join.output(), inlined.output());
    }

    public void testEmptyBuildKeepsEagerAntiJoinProbe() {
        var join = join(true, true, checked(leftStep, leftKey), relation(rightStep, rightKey), true);
        var empty = new LocalRelation(Source.EMPTY, join.right().output(), EmptyLocalSupplier.EMPTY);
        assertSame(join.left(), AbstractSubqueryJoin.inlineData(join, empty, 0, TestBlockFactory.getNonBreakingInstance(), null));
    }

    public void testNullBuildShortCircuitStillDrainsEagerProbe() {
        var factory = TestBlockFactory.getNonBreakingInstance();
        for (boolean anti : List.of(false, true)) {
            var join = join(anti, true, checked(leftStep, leftKey), relation(rightStep, rightKey), false);
            var page = new Page(factory.newConstantNullBlock(1));
            try {
                var nullBuild = new LocalRelation(Source.EMPTY, List.of(rightKey), LocalSupplier.of(page));
                LogicalPlan inlined = AbstractSubqueryJoin.inlineData(join, nullBuild, 0, factory, null);
                assertSame(join.left(), as(inlined, Drain.class).child());
            } finally {
                page.releaseBlocks();
            }
        }
    }

    public void testStaticEmptyOperandCannotPruneSiblingValidation() {
        for (boolean anti : List.of(false, true)) {
            for (boolean emptyLeft : List.of(false, true)) {
                LogicalPlan left = emptyLeft
                    ? new LocalRelation(Source.EMPTY, List.of(leftStep, leftKey), EmptyLocalSupplier.EMPTY)
                    : checked(leftStep, leftKey);
                LogicalPlan right = emptyLeft
                    ? checked(rightStep, rightKey)
                    : new LocalRelation(Source.EMPTY, List.of(rightStep, rightKey), EmptyLocalSupplier.EMPTY);
                var join = join(anti, true, left, right, true);
                LogicalPlan optimized = new LogicalPlanOptimizer(unboundLogicalOptimizerContext(TransportVersion.current())).optimize(join);
                var retained = as(optimized, AbstractSubqueryJoin.class);
                assertTrue(retained.eager());
                assertEquals(join.output(), retained.output());
                LogicalPlan nonemptyOperand = emptyLeft ? retained.right() : retained.left();
                assertTrue(nonemptyOperand.anyMatch(CheckSeriesUniqueness.class::isInstance));
                assertTrue(nonemptyOperand.anyMatch(EsRelation.class::isInstance));
            }
        }
    }

    public void testEagerPolicySurvivesPlanRewrites() {
        for (boolean anti : List.of(false, true)) {
            var join = join(anti, true, checked(leftStep, leftKey), relation(rightStep, rightKey), true);
            var replaced = as(join.replaceChildren(relation(leftStep, leftKey), checked(rightStep, rightKey)), AbstractSubqueryJoin.class);
            assertTrue(replaced.eager());
            var remapped = as(
                join.transformExpressionsOnly(Attribute.class, a -> a.withName(a.name() + "_renamed")),
                AbstractSubqueryJoin.class
            );
            assertTrue(remapped.eager());
            var changedPolicy = as(join.transformPropertiesOnly(Boolean.class, ignored -> false), AbstractSubqueryJoin.class);
            assertFalse(changedPolicy.eager());
            assertNotEquals(join, changedPolicy);
            assertEquals(join.left(), changedPolicy.left());
            assertEquals(join.right(), changedPolicy.right());
            assertEquals(join, changedPolicy.transformPropertiesOnly(Boolean.class, ignored -> true));
            var remappedDefault = as(
                changedPolicy.transformExpressionsOnly(Attribute.class, a -> a.withName(a.name() + "_renamed")),
                AbstractSubqueryJoin.class
            );
            assertFalse(remappedDefault.eager());
        }
    }

    private AbstractSubqueryJoin join(boolean anti, boolean eager, LogicalPlan left, LogicalPlan right, boolean compositeKey) {
        List<Attribute> leftFields = compositeKey ? List.of(leftStep, leftKey) : List.of(leftKey);
        List<Attribute> rightFields = compositeKey ? List.of(rightStep, rightKey) : List.of(rightKey);
        return anti
            ? new AntiJoin(Source.EMPTY, left, right, leftFields, rightFields, eager)
            : new SemiJoin(Source.EMPTY, left, right, leftFields, rightFields, eager);
    }

    private LogicalPlan checked(Attribute step, Attribute key) {
        return new CheckSeriesUniqueness(Source.EMPTY, relation(step, key), step, key);
    }

    private EsRelation relation(Attribute step, Attribute key) {
        return new EsRelation(
            Source.EMPTY,
            "metrics",
            IndexMode.STANDARD,
            Map.of("", List.of("metrics")),
            Map.of("", List.of("metrics")),
            Map.of("metrics", new IndexProperties(IndexMode.STANDARD, 1)),
            List.of(step, key)
        );
    }
}
