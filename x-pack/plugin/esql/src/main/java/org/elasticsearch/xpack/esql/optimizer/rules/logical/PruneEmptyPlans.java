/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.plan.logical.Drain;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.promql.CheckSeriesUniqueness;

public final class PruneEmptyPlans extends OptimizerRules.OptimizerRule<UnaryPlan> {

    /** Removes unused results without suppressing validations that must consume their input. */
    public static LogicalPlan skipPlan(UnaryPlan plan) {
        if (plan instanceof Drain || plan instanceof CheckSeriesUniqueness) {
            return plan;
        }
        LogicalPlan child = plan.child();
        if (child.anyMatch(
            node -> node instanceof Drain || node instanceof CheckSeriesUniqueness || node instanceof Join join && join.eager()
        )) {
            // False filters and zero limits preserve their child's schema and can discard its rows after execution.
            // Pruning other unary nodes is optional: retain their output definitions rather than inventing attributes.
            if (plan.output().equals(child.output()) == false) {
                return plan;
            }
            return child instanceof Drain ? child : new Drain(plan.source(), child);
        }
        return new LocalRelation(plan.source(), plan.output(), EmptyLocalSupplier.EMPTY);
    }

    @Override
    protected LogicalPlan rule(UnaryPlan plan) {
        return plan.output().isEmpty() ? skipPlan(plan) : plan;
    }
}
