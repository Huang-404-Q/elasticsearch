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
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.PushDownAndCombineLimits;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.promql.CheckSeriesUniqueness;
import org.elasticsearch.xpack.esql.plan.physical.DistinctByExec;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.LocalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.PackDimsExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.session.Versioned;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.unboundLogicalOptimizerContext;

public class CheckSeriesUniquenessMapperTests extends ESTestCase {
    private final Attribute step = new ReferenceAttribute(Source.EMPTY, "step", DataType.DATETIME);
    private final Attribute identity = new ReferenceAttribute(Source.EMPTY, "identity", DataType.KEYWORD);
    private final Attribute value = new ReferenceAttribute(Source.EMPTY, "value", DataType.DOUBLE);

    public void testLocalInputUsesExactStepAndIdentityKey() {
        LocalRelation child = new LocalRelation(Source.EMPTY, List.of(step, identity, value), EmptyLocalSupplier.EMPTY);
        EvalExec evaluated = assertGuard(new CheckSeriesUniqueness(Source.EMPTY, child, step, identity));
        assertEquals(child.output(), as(evaluated.child(), LocalSourceExec.class).output());
    }

    public void testRemoteInputIsGatheredBeforeCheckingUniqueness() {
        EsRelation child = new EsRelation(
            Source.EMPTY,
            "metrics",
            IndexMode.TIME_SERIES,
            Map.of(),
            Map.of(),
            Map.of(),
            List.of(step, identity, value)
        );
        EvalExec evaluated = assertGuard(new CheckSeriesUniqueness(Source.EMPTY, child, step, identity));
        ExchangeExec exchange = as(evaluated.child(), ExchangeExec.class);
        assertSame(child, as(exchange.child(), FragmentExec.class).fragment());
    }

    public void testLimitCannotBePushedBelowUniquenessCheck() {
        LogicalPlan child = new LocalRelation(Source.EMPTY, List.of(step, identity, value), EmptyLocalSupplier.EMPTY);
        var check = new CheckSeriesUniqueness(Source.EMPTY, child, step, identity);
        var limit = new Limit(Source.EMPTY, new Literal(Source.EMPTY, 1, DataType.INTEGER), check);
        assertSame(limit, new PushDownAndCombineLimits().rule(limit, unboundLogicalOptimizerContext()));
    }

    private EvalExec assertGuard(CheckSeriesUniqueness check) {
        PhysicalPlan physical = new Mapper().map(new Versioned<>(check, TransportVersion.current()));
        ProjectExec project = as(physical, ProjectExec.class);
        assertEquals(check.output(), project.output());
        DistinctByExec distinct = as(project.child(), DistinctByExec.class);
        assertTrue(distinct.failOnDuplicate());
        PackDimsExec key = as(distinct.child(), PackDimsExec.class);
        assertSame(key.packed(), distinct.key());
        EvalExec evaluated = as(key.child(), EvalExec.class);
        assertEquals(List.of(step, identity), evaluated.fields().stream().map(alias -> alias.child()).toList());
        assertEquals(evaluated.fields().stream().map(alias -> alias.toAttribute()).toList(), key.dims());
        return evaluated;
    }
}
