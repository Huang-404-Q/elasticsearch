/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.timeseries;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.esql.expression.function.Param;

import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;

/**
 * Builds the complete canonical label-set key. Byte-based hash tables hash these bytes and compare
 * them on collisions, so grouping and matching never confuse distinct label sets.
 */
public final class TimeSeriesHash extends TimeSeriesFunction implements OptionalArgument {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "TimeSeriesHash",
        TimeSeriesHash::new
    );
    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(TimeSeriesHash.class)
        .unaryVariadic(TimeSeriesHash::new)
        .name("ts_hash");

    @FunctionInfo(
        returnType = "keyword",
        appliesTo = @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.EXPERIMENTAL),
        briefSummary = "Builds a canonical key from time-series metadata and promoted labels.",
        description = "Builds a canonical key from time-series metadata and promoted labels."
    )
    public TimeSeriesHash(
        Source source,
        @Param(
            name = "metadata",
            type = "_timeseries",
            description = "Time-series metadata; null denotes the empty label set."
        ) Expression metadata,
        @Param(
            name = "labels",
            type = { "keyword", "text", "boolean", "integer", "long", "unsigned_long", "double", "ip" },
            description = "Constant label names and corresponding promoted values, in alternating pairs.",
            optional = true
        ) List<Expression> labels
    ) {
        super(source, Stream.concat(Stream.of(metadata), labels.stream()).toList());
    }

    private TimeSeriesHash(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public DataType dataType() {
        return DataType.KEYWORD;
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        return TimeSeriesEvaluator.factory(TimeSeriesEvaluator.Operation.HASH, children(), toEvaluator);
    }

    @Override
    protected TypeResolution resolveArguments() {
        return namedValues();
    }

    @Override
    public TimeSeriesHash replaceChildren(List<Expression> children) {
        return new TimeSeriesHash(source(), children.getFirst(), children.subList(1, children.size()));
    }

    @Override
    protected NodeInfo<TimeSeriesHash> info() {
        return NodeInfo.create(this, TimeSeriesHash::new, metadata(), children().subList(1, children().size()));
    }
}
