/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.gds.core.io;

import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.api.schema.PropertySchema;
import org.neo4j.gds.core.io.file.GraphPropertyVisitor;
import org.neo4j.gds.utils.CloseableThreadLocal;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.BaseStream;
import java.util.stream.DoubleStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public final class GraphStoreGraphPropertyVisitor extends GraphPropertyVisitor {

    private final Map<String, PropertySchema> graphPropertySchema;
    private final CloseableThreadLocal<Map<String, StreamBuilder<?>>> streamBuilders;
    private final ReentrantLock lock;
    private final Map<String, List<StreamBuilder<?>>> streamFractions;

    public GraphStoreGraphPropertyVisitor(Map<String, PropertySchema> graphPropertySchema) {
        this.graphPropertySchema = graphPropertySchema;
        this.streamBuilders = CloseableThreadLocal.withInitial(HashMap::new);
        this.lock = new ReentrantLock();
        this.streamFractions = new HashMap<>();
    }

    @Override
    public boolean property(String key, Object value) {
        appendToStream(key, value, graphPropertySchema.get(key).valueType());
        return false;
    }

    @Override
    public void flush() throws IOException {
        try {
            lock.lock();
            var threadLocalStreamBuilder = streamBuilders.get();
            threadLocalStreamBuilder.forEach((propertyName, streamBuilder) -> streamFractions
                .computeIfAbsent(propertyName, __ -> new ArrayList<>())
                .add(streamBuilder));
            threadLocalStreamBuilder.clear();
        } finally {
            lock.unlock();
        }
    }

    public Map<String, List<StreamBuilder<?>>> streamFractions() {
        return this.streamFractions;
    }

    @Override
    public void close() {
        streamBuilders.close();
    }

    private void appendToStream(String key, Object value, ValueType valueType) {
        streamBuilders
            .get()
            .computeIfAbsent(key, __ -> StreamBuilder.forType(valueType))
            .add(value);
    }

    public static class Builder {
        private Map<String, PropertySchema> graphPropertySchema;

        public void withGraphPropertySchema(Map<String, PropertySchema> graphPropertySchema) {
            this.graphPropertySchema = graphPropertySchema;
        }

        public GraphStoreGraphPropertyVisitor build() {
            return new GraphStoreGraphPropertyVisitor(graphPropertySchema);
        }
    }

    public interface StreamBuilder<T extends BaseStream<?, T>> {
        void add(Object value);
        ReducibleStream<T> build();

        static StreamBuilder<?> forType(ValueType valueType) {
            switch (valueType) {
                case DOUBLE:
                    return new DoubleStreamBuilder();
                case LONG:
                    return new LongStreamBuilder();
                default:
                    return new ObjectStreamBuilder<>(valueType);
            }
        }
    }

    public interface ReducibleStream<T extends BaseStream<?, T>> {
        T stream();
        ValueType valueType();
        ReducibleStream<T> reduce(ReducibleStream<T> other);
    }

    static class LongStreamBuilder implements StreamBuilder<LongStream> {

        private final LongStream.Builder longStreamBuilder;

        LongStreamBuilder() {
            this.longStreamBuilder = LongStream.builder();
        }

        @Override
        public void add(Object value) {
            this.longStreamBuilder.accept((long) value);
        }

        @Override
        public ReducibleStream<LongStream> build() {
            return new ReducibleLongStream(this.longStreamBuilder.build());
        }
    }

    static class ReducibleLongStream implements ReducibleStream<LongStream> {

        private final LongStream stream;

        ReducibleLongStream(LongStream stream) {
            this.stream = stream;
        }

        @Override
        public LongStream stream() {
            return this.stream;
        }

        @Override
        public ValueType valueType() {
            return ValueType.LONG;
        }

        @Override
        public ReducibleStream<LongStream> reduce(ReducibleStream<LongStream> other) {
            return new ReducibleLongStream(LongStream.concat(stream(), other.stream()));
        }
    }

    static class DoubleStreamBuilder implements StreamBuilder<DoubleStream> {

        private final DoubleStream.Builder doubleStreamBuilder;

        DoubleStreamBuilder() {
            this.doubleStreamBuilder = DoubleStream.builder();
        }

        @Override
        public void add(Object value) {
            this.doubleStreamBuilder.add((double) value);
        }

        @Override
        public ReducibleStream<DoubleStream> build() {
            return new ReducibleDoubleStream(this.doubleStreamBuilder.build());
        }
    }

    static class ReducibleDoubleStream implements ReducibleStream<DoubleStream> {

        private final DoubleStream stream;

        ReducibleDoubleStream(DoubleStream stream) {
            this.stream = stream;
        }

        @Override
        public DoubleStream stream() {
            return this.stream;
        }

        @Override
        public ValueType valueType() {
            return ValueType.DOUBLE;
        }

        @Override
        public ReducibleStream<DoubleStream> reduce(ReducibleStream<DoubleStream> other) {
            return new ReducibleDoubleStream(DoubleStream.concat(stream(), other.stream()));
        }
    }

    static class ObjectStreamBuilder<T> implements StreamBuilder<Stream<T>> {

        private final Stream.Builder<T> streamBuilder;
        private final ValueType valueType;

        ObjectStreamBuilder(ValueType valueType) {
            this.valueType = valueType;
            this.streamBuilder = Stream.builder();
        }

        @Override
        public void add(Object value) {
            this.streamBuilder.add((T) value);
        }

        @Override
        public ReducibleStream<Stream<T>> build() {
            return new ReducibleObjectStream<>(this.streamBuilder.build(), valueType);
        }
    }

    static class ReducibleObjectStream<T> implements ReducibleStream<Stream<T>> {

        private final Stream<T> stream;
        private final ValueType valueType;

        ReducibleObjectStream(Stream<T> stream, ValueType valueType) {
            this.stream = stream;
            this.valueType = valueType;
        }

        @Override
        public Stream<T> stream() {
            return this.stream;
        }

        @Override
        public ValueType valueType() {
            return this.valueType;
        }

        @Override
        public ReducibleStream<Stream<T>> reduce(ReducibleStream<Stream<T>> other) {
            return new ReducibleObjectStream<>(Stream.concat(stream(), other.stream()), valueType);
        }
    }
}
