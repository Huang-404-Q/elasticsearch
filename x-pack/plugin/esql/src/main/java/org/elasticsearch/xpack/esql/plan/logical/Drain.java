/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * Consumes the complete child without returning rows. Unlike a constant-false filter, the child must still execute
 * its validations. Lowering happens on the coordinator after logical pruning, using the ordinary filter operator.
 */
public final class Drain extends UnaryPlan implements PipelineBreaker, ExecutesOn.Coordinator, SortPreserving {
    /** Defines an eager evaluation whose samples are discarded. */
    public Drain(Source source, LogicalPlan child) {
        super(source, child);
    }

    @Override
    protected AttributeSet computeReferences() {
        // Discarding the result must not make the child's computations unused during column pruning.
        return child().outputSet();
    }

    @Override
    public boolean expressionsResolved() {
        return true;
    }

    @Override
    public boolean skipTelemetry() {
        return true;
    }

    @Override
    public Drain replaceChild(LogicalPlan child) {
        return new Drain(source(), child);
    }

    @Override
    protected NodeInfo<Drain> info() {
        return NodeInfo.create(this, Drain::new, child());
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("Drain runs on the coordinator and is not serialized");
    }

    @Override
    public void writeTo(StreamOutput out) {
        throw new UnsupportedOperationException("Drain runs on the coordinator and is not serialized");
    }
}
