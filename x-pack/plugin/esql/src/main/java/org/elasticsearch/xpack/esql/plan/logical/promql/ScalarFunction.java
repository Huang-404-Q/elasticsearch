/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionDefinition;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionRegistry;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionRegistry.PromqlContext;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.plan.logical.LeafPlan;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContext.IntermediateResult;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContext.supportTimeSeriesMetadataType;

/**
 * Represents a PromQL function call with no input argument, evaluated on the command's clock.
 * <p>
 * Functions such as {@code time()} and {@code pi()} return scalars. Date functions called without their
 * optional vector argument return an instant vector with an empty label set, as if passed {@code vector(time())}.
 * This corresponds to PromQL syntax:
 * <pre>
 * scalar_function()
 * </pre>
 * Examples: {@code time()}, {@code pi()}
 */
public final class ScalarFunction extends LeafPlan implements PromqlPlan {

    private final PromqlFunctionDefinition definition;

    public ScalarFunction(Source source, PromqlFunctionDefinition definition) {
        super(source);
        this.definition = definition;
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(this, ScalarFunction::new, definition);
    }

    @Override
    public boolean expressionsResolved() {
        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(definition);
    }

    @Override
    public boolean equals(Object obj) {
        return this == obj || (obj instanceof ScalarFunction other && Objects.equals(definition, other.definition));
    }

    @Override
    public List<Attribute> output() {
        return List.of();
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("does not support serialization");
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("does not support serialization");
    }

    @Override
    public PromqlDataType returnType() {
        return definition.functionType().outputType;
    }

    public String functionName() {
        return definition.name();
    }

    public PromqlFunctionDefinition definition() {
        return definition;
    }

    /**
     * Builds the ES|QL expression that implements this PromQL scalar function.
     *
     * @param ctx the PromQL evaluation context (timestamp, window, step, configuration)
     */
    public Expression buildEsqlFunction(PromqlFunctionRegistry.PromqlContext ctx) {
        try {
            Object built = definition.esqlBuilder().build(source(), null, ctx, List.of());
            assert built instanceof Expression : "Function [" + functionName() + "] is not lowered to an expression";
            return (Expression) built;
        } catch (ParsingException e) {
            throw e;
        } catch (Exception e) {
            String message = e.getMessage() != null ? e.getMessage() : e.toString();
            throw new ParsingException(source(), "Error building ESQL function for [{}]: {}", functionName(), message);
        }
    }

    /** Evaluates a source-free function once per evaluation step, independently of source series and selector predicates. */
    @Override
    public IntermediateResult translate(TranslationContext context) {
        if (supportTimeSeriesMetadataType(context)) {
            // Establish the clock before building the expression: source-derived steps may come from several
            // offset branches, whose individual step attributes are no longer available after deduplication.
            IntermediateResult clock = context.inlineConstantSteps(
                new IntermediateResult(
                    context.cmd().child(),
                    TranslationSchema.EMPTY,
                    Literal.fromDouble(source(), 0.0),
                    context.stepAttr(),
                    null,
                    IntermediateResult.Kind.BEFORE_INITIAL_AGGREGATE,
                    Map.of()
                )
            );
            return context.eval(clock, buildEsqlFunction(new PromqlContext(context.time(), null, clock.step(), context.configuration())));
        }
        var function = buildEsqlFunction(
            new PromqlContext(context.cmd().timestamp(), null, context.cmd().stepAttribute(), context.configuration())
        );
        return new IntermediateResult(context.cmd().child(), TranslationSchema.EMPTY, function, context.stepAttr());
    }
}
