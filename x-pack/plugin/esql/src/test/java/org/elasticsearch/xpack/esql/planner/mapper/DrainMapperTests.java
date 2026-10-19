/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner.mapper;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.index.IndexProperties;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalPlanOptimizer;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.PruneColumns;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.PushDownAndCombineLimits;
import org.elasticsearch.xpack.esql.plan.logical.Drain;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.promql.CheckSeriesUniqueness;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.LocalSourceExec;
import org.elasticsearch.xpack.esql.session.Versioned;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_CFG;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.unboundLogicalOptimizerContext;

public class DrainMapperTests extends ESTestCase {
    private final Attribute step = new ReferenceAttribute(Source.EMPTY, "step", DataType.DATETIME);
    private final Attribute identity = new ReferenceAttribute(Source.EMPTY, "identity", DataType.KEYWORD);
    private final Attribute value = new ReferenceAttribute(Source.EMPTY, "value", DataType.DOUBLE);

    public void testLocalInputIsConsumedWithoutChangingOutputColumns() {
        LocalRelation child = new LocalRelation(Source.EMPTY, List.of(step, identity, value), EmptyLocalSupplier.EMPTY);
        FilterExec filter = map(child);
        assertEquals(Literal.FALSE, filter.condition());
        assertEquals(child.output(), filter.output());
        assertEquals(child.output(), as(filter.child(), LocalSourceExec.class).output());
    }

    public void testPhysicalOptimizerKeepsDiscardingFilterOnCoordinator() {
        EsRelation child = relation();
        FilterExec filter = map(child);
        assertSame(child, as(as(filter.child(), ExchangeExec.class).child(), FragmentExec.class).fragment());

        var optimizer = new PhysicalPlanOptimizer(new PhysicalOptimizerContext(TEST_CFG, TransportVersion.current()));
        FilterExec optimized = as(optimizer.optimize(filter), FilterExec.class);
        assertEquals(Literal.FALSE, optimized.condition());
        var fragment = as(as(optimized.child(), ExchangeExec.class).child(), FragmentExec.class).fragment();
        assertTrue(fragment.anyMatch(plan -> plan == child));
        assertFalse(fragment.anyMatch(Drain.class::isInstance));
    }

    public void testLimitCannotBypassChildEvaluation() {
        var drain = new Drain(Source.EMPTY, relation());
        var limit = new Limit(Source.EMPTY, new Literal(Source.EMPTY, 1, DataType.INTEGER), drain);
        assertSame(limit, new PushDownAndCombineLimits().rule(limit, unboundLogicalOptimizerContext()));
    }

    public void testLogicalOptimizationKeepsChildValidation() {
        var check = new CheckSeriesUniqueness(Source.EMPTY, relation(), step, identity);
        var drain = new Drain(Source.EMPTY, check);
        var limit = new Limit(Source.EMPTY, new Literal(Source.EMPTY, 1, DataType.INTEGER), drain);
        LogicalPlan optimized = new LogicalPlanOptimizer(unboundLogicalOptimizerContext(TransportVersion.current())).optimize(limit);
        Drain optimizedDrain = as(as(optimized, Limit.class).child(), Drain.class);
        assertEquals(check.output(), optimizedDrain.output());
        assertTrue(optimizedDrain.child().anyMatch(CheckSeriesUniqueness.class::isInstance));
        assertFalse(optimizedDrain.child().anyMatch(Limit.class::isInstance));
        assertTrue(optimizedDrain.child().anyMatch(EsRelation.class::isInstance));
    }

    public void testColumnPruningRetainsDiscardedComputations() {
        var alias = new Alias(Source.EMPTY, "discarded_value", value);
        var eval = new Eval(Source.EMPTY, relation(), List.of(alias));
        var project = new Project(Source.EMPTY, new Drain(Source.EMPTY, eval), List.of(identity));
        LogicalPlan pruned = new PruneColumns().apply(project);
        Eval retained = as(as(as(pruned, Project.class).child(), Drain.class).child(), Eval.class);
        assertEquals(List.of(alias), retained.fields());
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

    private FilterExec map(LogicalPlan child) {
        return as(new Mapper().map(new Versioned<>(new Drain(Source.EMPTY, child), TransportVersion.current())), FilterExec.class);
    }
}
