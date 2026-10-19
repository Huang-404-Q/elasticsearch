/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.index.fielddata.FormattedDocValues;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.LeafNumericFieldData;
import org.elasticsearch.index.fielddata.SortableBinaryDocValues;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.blockloader.docvalues.BlockDocValuesReader;
import org.elasticsearch.index.mapper.flattened.FlattenedFieldMapper;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.xpack.esql.core.util.TimeSeriesMetadata;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlLabels;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;

/**
 * Reads typed time-series metadata from indexed dimensions. Source filtering and numeric parsing can
 * change keyword label spellings, so metadata and promoted columns must use the same indexed values.
 */
final class TimeSeriesMetadataBlockLoader extends BlockDocValuesReader.DocValuesBlockLoader {
    private record Dimension(String label, IndexFieldData<?> fieldData, DocValueFormat format) {}

    private final List<Dimension> dimensions;

    TimeSeriesMetadataBlockLoader(SearchExecutionContext context, Set<String> excludedFields) {
        var excluded = new HashSet<String>();
        for (String name : excludedFields) {
            MappedFieldType field = context.getFieldType(name);
            excluded.add(
                field instanceof FlattenedFieldMapper.KeyedFlattenedFieldType keyed ? keyed.rootName() + "." + keyed.key()
                    : field == null ? name
                    : field.name()
            );
        }
        var dimensions = new ArrayList<Dimension>();
        var labels = new HashSet<String>();
        for (var mapper : context.getMappingLookup().dimensionFieldMappers().values()) {
            MappedFieldType field = mapper.fieldType();
            if (field instanceof FlattenedFieldMapper.RootFlattenedFieldType flattened) {
                for (String key : flattened.dimensions()) {
                    String name = field.name() + "." + key;
                    validateLabelName(labels, name);
                    if (excluded.contains(field.name()) == false && excluded.contains(name) == false) {
                        addDimension(dimensions, name, context.getFieldType(name), context);
                    }
                }
            } else {
                validateLabelName(labels, field.name());
                if (excluded.contains(field.name()) == false) {
                    addDimension(dimensions, field.name(), field, context);
                }
            }
        }
        this.dimensions = List.copyOf(dimensions);
    }

    private static void validateLabelName(Set<String> labels, String name) {
        String label = PromqlLabels.labelName(name);
        if (labels.add(label) == false) {
            throw new IllegalArgumentException("Multiple dimension fields resolve to time-series label [" + label + "]");
        }
    }

    private static void addDimension(List<Dimension> dimensions, String name, MappedFieldType field, SearchExecutionContext context) {
        if (field.hasDocValues() == false) {
            throw new IllegalArgumentException("Time-series dimension [" + name + "] must have doc values");
        }
        dimensions.add(
            new Dimension(
                PromqlLabels.labelName(name),
                context.getForField(field, MappedFieldType.FielddataOperation.SEARCH),
                field.docValueFormat(null, null)
            )
        );
    }

    @Override
    public Builder builder(BlockFactory factory, int expectedCount) {
        return factory.bytesRefs(expectedCount);
    }

    @Override
    public ColumnAtATimeReader reader(CircuitBreaker breaker, LeafReaderContext context) throws IOException {
        return new Reader(breaker, context);
    }

    @Override
    public RowStrideReader rowStrideReader(CircuitBreaker breaker, LeafReaderContext context) throws IOException {
        return new Reader(breaker, context);
    }

    @Override
    public String toString() {
        return "TimeSeriesMetadataFromDocValues";
    }

    private final class Reader extends BlockDocValuesReader implements RowStrideReader {
        private final FormattedDocValues[] numericValues;
        private final SortableBinaryDocValues[] binaryValues;
        private final BreakingBytesRefBuilder buffer;
        private final long readerReservation;
        private long rowReservation;
        private int docId = -1;
        private boolean closed;

        private Reader(CircuitBreaker breaker, LeafReaderContext context) {
            super(breaker);
            readerReservation = 256L + 64L * dimensions.size();
            breaker.addEstimateBytesAndMaybeBreak(readerReservation, "time-series metadata readers");
            BreakingBytesRefBuilder buffer = null;
            try {
                numericValues = new FormattedDocValues[dimensions.size()];
                binaryValues = new SortableBinaryDocValues[dimensions.size()];
                for (int i = 0; i < dimensions.size(); i++) {
                    Dimension dimension = dimensions.get(i);
                    var leaf = dimension.fieldData().load(context);
                    if (leaf instanceof LeafNumericFieldData) {
                        // In particular, unsigned longs must not pass through the fielddata's double-based byte view.
                        numericValues[i] = leaf.getFormattedValues(dimension.format());
                    } else {
                        binaryValues[i] = leaf.getBytesValues();
                    }
                }
                buffer = new BreakingBytesRefBuilder(breaker, "time-series metadata");
                this.buffer = buffer;
            } catch (RuntimeException | Error e) {
                if (buffer != null) {
                    buffer.close();
                }
                breaker.addWithoutBreaking(-readerReservation);
                throw e;
            }
        }

        @Override
        protected int docId() {
            return docId;
        }

        @Override
        public Block read(BlockFactory factory, Docs docs, int offset, boolean nullsFiltered) throws IOException {
            try (var builder = factory.bytesRefs(docs.count() - offset)) {
                for (int i = offset; i < docs.count(); i++) {
                    read(docs.get(i), null, builder);
                }
                return builder.build();
            }
        }

        @Override
        public void read(int docId, StoredFields storedFields, Builder builder) throws IOException {
            assert canReuse(docId);
            this.docId = docId;
            try {
                reserve(TimeSeriesMetadata.encodingScratchBytes() + 256L + 256L * dimensions.size());
                var labels = new TreeMap<String, String>();
                for (int i = 0; i < dimensions.size(); i++) {
                    Dimension dimension = dimensions.get(i);
                    Object value;
                    if (numericValues[i] != null) {
                        FormattedDocValues values = numericValues[i];
                        if (values.advanceExact(docId) == false) {
                            continue;
                        }
                        checkSingleValue(dimension.label(), values.docValueCount());
                        reserve(256L);
                        value = values.nextValue();
                    } else {
                        SortableBinaryDocValues values = binaryValues[i];
                        if (values.advanceExact(docId) == false) {
                            continue;
                        }
                        checkSingleValue(dimension.label(), values.docValueCount());
                        BytesRef bytes = values.nextValue();
                        // Reserve before UTF-8 decoding/formatting; retain the strings until the dictionary has been written.
                        reserve(128L + 4L * bytes.length);
                        value = dimension.format().format(bytes);
                    }
                    labels.put(dimension.label(), value instanceof BytesRef bytes ? bytes.utf8ToString() : value.toString());
                }
                buffer.clear();
                TimeSeriesMetadata.of(labels).writeTo(new OutputStream() {
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
                });
                ((BytesRefBuilder) builder).appendBytesRef(buffer.bytesRefView());
            } finally {
                breaker.addWithoutBreaking(-rowReservation);
                rowReservation = 0;
            }
        }

        private static void checkSingleValue(String label, int count) {
            if (count != 1) {
                throw new IllegalArgumentException("Time-series label [" + label + "] must be single-valued");
            }
        }

        private void reserve(long bytes) {
            breaker.addEstimateBytesAndMaybeBreak(bytes, "time-series metadata normalization");
            rowReservation += bytes;
        }

        @Override
        public void close() {
            if (closed == false) {
                closed = true;
                buffer.close();
                breaker.addWithoutBreaking(-readerReservation);
            }
        }

        @Override
        public String toString() {
            return TimeSeriesMetadataBlockLoader.this.toString();
        }
    }
}
