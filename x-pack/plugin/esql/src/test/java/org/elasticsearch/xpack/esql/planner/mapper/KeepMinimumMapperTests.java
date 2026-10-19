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
import org.elasticsearch.xpack.esql.plan.logical.KeepMinimum;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.KeepMinimumExec;
import org.elasticsearch.xpack.esql.plan.physical.LocalSourceExec;
import org.elasticsearch.xpack.esql.session.Versioned;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.unboundLogicalOptimizerContext;

public class KeepMinimumMapperTests extends ESTestCase {
    private final Attribute key = new ReferenceAttribute(Source.EMPTY, "key", DataType.KEYWORD);
    private final Attribute priority = new ReferenceAttribute(Source.EMPTY, "priority", DataType.INTEGER);
    private final Attribute value = new ReferenceAttribute(Source.EMPTY, "value", DataType.DOUBLE);

    public void testLocalInputPreservesAllColumns() {
        LocalRelation child = new LocalRelation(Source.EMPTY, List.of(key, priority, value), EmptyLocalSupplier.EMPTY);
        KeepMinimumExec keep = map(child);
        assertEquals(child.output(), keep.output());
        assertEquals(child.output(), as(keep.child(), LocalSourceExec.class).output());
        assertEquals(key, keep.key());
        assertEquals(priority, keep.priority());
    }

    public void testRemoteInputIsGatheredBeforeSelectingMinimum() {
        EsRelation child = new EsRelation(
            Source.EMPTY,
            "metrics",
            IndexMode.TIME_SERIES,
            Map.of(),
            Map.of(),
            Map.of(),
            List.of(key, priority, value)
        );
        KeepMinimumExec keep = map(child);
        assertSame(child, as(as(keep.child(), ExchangeExec.class).child(), FragmentExec.class).fragment());
    }

    public void testLimitCannotBypassLateMinimum() {
        LogicalPlan child = new LocalRelation(Source.EMPTY, List.of(key, priority, value), EmptyLocalSupplier.EMPTY);
        var keep = new KeepMinimum(Source.EMPTY, child, key, priority);
        var limit = new Limit(Source.EMPTY, new Literal(Source.EMPTY, 1, DataType.INTEGER), keep);
        assertSame(limit, new PushDownAndCombineLimits().rule(limit, unboundLogicalOptimizerContext()));
    }

    private KeepMinimumExec map(LogicalPlan child) {
        return as(
            new Mapper().map(new Versioned<>(new KeepMinimum(Source.EMPTY, child, key, priority), TransportVersion.current())),
            KeepMinimumExec.class
        );
    }
}
