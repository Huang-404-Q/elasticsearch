/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.timeseries;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.TimeSeriesMetadata;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper.ToEvaluator;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.core.util.NumericUtils.unsignedLongAsBigInteger;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.ipToString;

/**
 * Evaluates metadata operations with heterogeneous label values. Labels are folded once; values are
 * evaluated per row, even when several rows share the same metadata dictionary entry.
 */
final class TimeSeriesEvaluator implements ExpressionEvaluator {
    enum Operation {
        GET,
        SET,
        UNSET,
        HASH,
        TO_STRING
    }

    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(TimeSeriesEvaluator.class);

    private final Operation operation;
    private final ExpressionEvaluator[] inputs;
    private final List<String> names;
    private final DataType[] valueTypes;
    private final DriverContext context;

    private TimeSeriesEvaluator(
        Operation operation,
        ExpressionEvaluator[] inputs,
        List<String> names,
        DataType[] valueTypes,
        DriverContext context
    ) {
        this.operation = operation;
        this.inputs = inputs;
        this.names = names;
        this.valueTypes = valueTypes;
        this.context = context;
    }

    static Factory factory(Operation operation, List<Expression> arguments, ToEvaluator toEvaluator) {
        boolean hasValues = switch (operation) {
            case SET, HASH, TO_STRING -> true;
            case GET, UNSET -> false;
        };
        int count = (arguments.size() - 1) / (hasValues ? 2 : 1);
        String[] names = new String[count];
        DataType[] valueTypes = new DataType[hasValues ? count : 0];
        ExpressionEvaluator.Factory[] inputs = new ExpressionEvaluator.Factory[valueTypes.length + 1];
        inputs[0] = toEvaluator.apply(arguments.getFirst());
        for (int i = 0; i < count; i++) {
            int nameIndex = 1 + i * (hasValues ? 2 : 1);
            names[i] = ((BytesRef) arguments.get(nameIndex).fold(toEvaluator.foldCtx())).utf8ToString();
            if (hasValues) {
                Expression value = arguments.get(nameIndex + 1);
                inputs[i + 1] = toEvaluator.apply(value);
                valueTypes[i] = value.dataType();
            }
        }
        return new Factory(operation, inputs, List.of(names), valueTypes);
    }

    @Override
    public Block eval(Page page) {
        Block[] blocks = new Block[inputs.length];
        try (var release = Releasables.wrap(blocks)) {
            for (int i = 0; i < inputs.length; i++) {
                blocks[i] = inputs[i].eval(page);
            }
            return eval(page.getPositionCount(), blocks);
        }
    }

    private Block eval(int positions, Block[] blocks) {
        BytesRef scratch = new BytesRef();
        try (
            BytesRefBlock.Builder result = context.blockFactory().newBytesRefBlockBuilder(positions);
            BreakingBytesRefBuilder buffer = new BreakingBytesRefBuilder(context.breaker(), "time_series_metadata")
        ) {
            for (int p = 0; p < positions; p++) {
                long reservation = scratchBytes(blocks, p, scratch);
                context.breaker().addEstimateBytesAndMaybeBreak(reservation, "time_series_metadata");
                try {
                    BytesRef metadata = blocks[0].isNull(p)
                        ? null
                        : ((BytesRefBlock) blocks[0]).getBytesRef(blocks[0].getFirstValueIndex(p), scratch);
                    TimeSeriesMetadata labels = TimeSeriesMetadata.read(metadata);
                    switch (operation) {
                        case GET -> {
                            String value = labels.get(names.getFirst());
                            if (value == null) {
                                result.appendNull();
                            } else {
                                result.appendBytesRef(new BytesRef(value));
                            }
                        }
                        case UNSET -> result.appendBytesRef(write(labels.unset(names), buffer));
                        case SET, HASH, TO_STRING -> {
                            Map<String, String> overlay = new HashMap<>();
                            for (int i = 0; i < valueTypes.length; i++) {
                                overlay.put(names.get(i), labelValue(blocks[i + 1], p, valueTypes[i], scratch));
                            }
                            result.appendBytesRef(write(labels.withLabels(overlay), buffer));
                        }
                    }
                } catch (IOException e) {
                    throw new UncheckedIOException("Invalid time-series metadata", e);
                } finally {
                    context.breaker().addWithoutBreaking(-reservation);
                }
            }
            return result.build();
        }
    }

    /**
     * Reserve scratch before decoding. The conservative multiplier covers decoded strings, parser
     * storage, and both source and transformed maps, including short labels with large object overhead.
     * Only one row's scratch is live; the output builder and blocks account their own storage.
     */
    private long scratchBytes(Block[] blocks, int position, BytesRef scratch) {
        long bytes = 0;
        for (int i = 0; i < blocks.length; i++) {
            Block block = blocks[i];
            if (block.isNull(position)) {
                continue;
            }
            if (block.getValueCount(position) != 1) {
                throw new IllegalArgumentException("Time-series metadata and label values must be single-valued");
            }
            bytes += block instanceof BytesRefBlock b ? b.getBytesRef(block.getFirstValueIndex(position), scratch).length : 32;
        }
        for (String name : names) {
            bytes += 2L * name.length() + 32;
        }
        return 4096 + TimeSeriesMetadata.encodingScratchBytes() + 64 * bytes;
    }

    private static String labelValue(Block block, int position, DataType type, BytesRef scratch) {
        if (block.isNull(position)) {
            return null;
        }
        int index = block.getFirstValueIndex(position);
        return switch (type) {
            case BOOLEAN -> Boolean.toString(((BooleanBlock) block).getBoolean(index));
            case INTEGER -> Integer.toString(((IntBlock) block).getInt(index));
            case LONG -> Long.toString(((LongBlock) block).getLong(index));
            case UNSIGNED_LONG -> unsignedLongAsBigInteger(((LongBlock) block).getLong(index)).toString();
            case DOUBLE -> Double.toString(((DoubleBlock) block).getDouble(index));
            case KEYWORD, TEXT -> ((BytesRefBlock) block).getBytesRef(index, scratch).utf8ToString();
            case IP -> ipToString(((BytesRefBlock) block).getBytesRef(index, scratch));
            default -> throw new IllegalStateException("Unsupported time-series label type [" + type + "]");
        };
    }

    private BytesRef write(TimeSeriesMetadata metadata, BreakingBytesRefBuilder buffer) throws IOException {
        buffer.clear();
        var output = new MetadataOutputStream(buffer);
        if (operation == Operation.TO_STRING) {
            metadata.renderTo(output);
        } else if (operation == Operation.HASH) {
            metadata.writeKeyTo(output);
        } else {
            metadata.writeTo(output);
        }
        return buffer.bytesRefView();
    }

    /** Prevents length overflow while the backing builder accounts all allocated output memory. */
    private static final class MetadataOutputStream extends OutputStream {
        private final BreakingBytesRefBuilder buffer;

        private MetadataOutputStream(BreakingBytesRefBuilder buffer) {
            this.buffer = buffer;
        }

        @Override
        public void write(int value) {
            checkLength(1);
            buffer.append((byte) value);
        }

        @Override
        public void write(byte[] bytes, int offset, int length) {
            checkLength(length);
            buffer.append(bytes, offset, length);
        }

        private void checkLength(int additional) {
            if ((long) buffer.length() + additional > ArrayUtil.MAX_ARRAY_LENGTH) {
                throw new IllegalArgumentException("Time-series metadata exceeds the maximum array size");
            }
        }
    }

    @Override
    public long baseRamBytesUsed() {
        long bytes = BASE_RAM_BYTES_USED + RamUsageEstimator.shallowSizeOf(names) + RamUsageEstimator.shallowSizeOf(inputs)
            + RamUsageEstimator.shallowSizeOf(valueTypes);
        for (String name : names) {
            bytes += RamUsageEstimator.sizeOf(name);
        }
        for (ExpressionEvaluator input : inputs) {
            bytes += input.baseRamBytesUsed();
        }
        return bytes;
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(inputs);
    }

    @Override
    public String toString() {
        return "TimeSeriesEvaluator[operation=" + operation + ", names=" + names + ", inputs=" + Arrays.toString(inputs) + "]";
    }

    record Factory(Operation operation, ExpressionEvaluator.Factory[] inputs, List<String> names, DataType[] valueTypes)
        implements
            ExpressionEvaluator.Factory {
        @Override
        public ExpressionEvaluator get(DriverContext context) {
            ExpressionEvaluator[] evaluators = new ExpressionEvaluator[inputs.length];
            boolean success = false;
            try {
                for (int i = 0; i < inputs.length; i++) {
                    evaluators[i] = inputs[i].get(context);
                }
                var evaluator = new TimeSeriesEvaluator(operation, evaluators, names, valueTypes, context);
                success = true;
                return evaluator;
            } finally {
                if (success == false) {
                    Releasables.closeExpectNoException(evaluators);
                }
            }
        }

        @Override
        public String toString() {
            return "TimeSeriesEvaluator[operation=" + operation + ", names=" + names + ", inputs=" + Arrays.toString(inputs) + "]";
        }
    }
}
