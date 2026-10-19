/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.Objects;

/** Coordinator execution of an all-ties minimum-priority filter. */
public final class KeepMinimumExec extends UnaryExec {
    private final Attribute key;
    private final Attribute priority;

    /** Binds the complete byte key and integer priority in the coordinator input. */
    public KeepMinimumExec(Source source, PhysicalPlan child, Attribute key, Attribute priority) {
        super(source, child);
        this.key = key;
        this.priority = priority;
    }

    /** The complete equality key. */
    public Attribute key() {
        return key;
    }

    /** The priority used to select every minimum-priority tie. */
    public Attribute priority() {
        return priority;
    }

    @Override
    public KeepMinimumExec replaceChild(PhysicalPlan child) {
        return new KeepMinimumExec(source(), child, key, priority);
    }

    @Override
    protected NodeInfo<KeepMinimumExec> info() {
        return NodeInfo.create(this, KeepMinimumExec::new, child(), key, priority);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), key, priority);
    }

    @Override
    public boolean equals(Object other) {
        return super.equals(other) && key.equals(((KeepMinimumExec) other).key) && priority.equals(((KeepMinimumExec) other).priority);
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("KeepMinimumExec runs on the coordinator and is not serialized");
    }

    @Override
    public void writeTo(StreamOutput out) {
        throw new UnsupportedOperationException("KeepMinimumExec runs on the coordinator and is not serialized");
    }
}
