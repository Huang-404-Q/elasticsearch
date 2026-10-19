/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.Objects;

/** Retains all rows tied at the minimum integer priority for each exact byte key. */
public final class KeepMinimum extends UnaryPlan implements PipelineBreaker, ExecutesOn.Coordinator, SortPreserving {
    private final Attribute key;
    private final Attribute priority;

    /** Defines the exact byte key and integer priority used to retain every minimum-priority tie. */
    public KeepMinimum(Source source, LogicalPlan child, Attribute key, Attribute priority) {
        super(source, child);
        this.key = key;
        this.priority = priority;
    }

    /** The complete equality key, not a potentially colliding digest. */
    public Attribute key() {
        return key;
    }

    /** The integer priority whose minimum is selected independently for each key. */
    public Attribute priority() {
        return priority;
    }

    @Override
    public boolean skipTelemetry() {
        return true;
    }

    @Override
    public boolean expressionsResolved() {
        return key.resolved() && priority.resolved();
    }

    @Override
    public KeepMinimum replaceChild(LogicalPlan child) {
        return new KeepMinimum(source(), child, key, priority);
    }

    @Override
    protected NodeInfo<KeepMinimum> info() {
        return NodeInfo.create(this, KeepMinimum::new, child(), key, priority);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), key, priority);
    }

    @Override
    public boolean equals(Object other) {
        return super.equals(other) && key.equals(((KeepMinimum) other).key) && priority.equals(((KeepMinimum) other).priority);
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("KeepMinimum runs on the coordinator and is not serialized");
    }

    @Override
    public void writeTo(StreamOutput out) {
        throw new UnsupportedOperationException("KeepMinimum runs on the coordinator and is not serialized");
    }
}
