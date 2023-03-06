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
package org.neo4j.gds.core.io.file;

import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.api.properties.graph.DoubleArrayGraphPropertyValues;
import org.neo4j.gds.api.properties.graph.DoubleGraphPropertyValues;
import org.neo4j.gds.api.properties.graph.FloatArrayGraphPropertyValues;
import org.neo4j.gds.api.properties.graph.GraphProperty;
import org.neo4j.gds.api.properties.graph.GraphPropertyStore;
import org.neo4j.gds.api.properties.graph.GraphPropertyValues;
import org.neo4j.gds.api.properties.graph.LongArrayGraphPropertyValues;
import org.neo4j.gds.api.properties.graph.LongGraphPropertyValues;
import org.neo4j.gds.api.schema.PropertySchema;
import org.neo4j.gds.core.io.GraphStoreGraphPropertyVisitor;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.DoubleStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

final class GraphPropertyStoreFromVisitorHelper {

    private GraphPropertyStoreFromVisitorHelper() {}

    static GraphPropertyStore fromGraphPropertyVisitor(Map<String, PropertySchema> graphPropertySchema, GraphStoreGraphPropertyVisitor graphStoreGraphPropertyVisitor) {
        var graphPropertyBuilder = GraphPropertyStore.builder();
        buildGraphPropertiesFromStreams(graphPropertyBuilder, graphPropertySchema, graphStoreGraphPropertyVisitor.streamFractions());
        return graphPropertyBuilder.build();
    }

    private static void buildGraphPropertiesFromStreams(
        GraphPropertyStore.Builder graphPropertyBuilder,
        Map<String, PropertySchema> graphPropertySchema,
        Map<String, List<GraphStoreGraphPropertyVisitor.StreamBuilder<?>>> streamFractions
    ) {
        streamFractions.forEach((key, streamBuilders) -> {
            var graphPropertyValues = getGraphPropertyValuesFromStream(graphPropertySchema.get(key).valueType(), streamBuilders);
            graphPropertyBuilder.putProperty(key, GraphProperty.of(key, graphPropertyValues));
        });
    }

    private static GraphStoreGraphPropertyVisitor.ReducibleStream<?> mergeStreamFractions(Collection<GraphStoreGraphPropertyVisitor.StreamBuilder<?>> streamFractions) {
        return streamFractions
            .stream()
            .map(GraphStoreGraphPropertyVisitor.StreamBuilder::build)
            .reduce(GraphStoreGraphPropertyVisitor.ReducibleStream::reduce)
            .orElseGet(GraphStoreGraphPropertyVisitor.ReducibleStream::empty);
    }

    private static GraphPropertyValues getGraphPropertyValuesFromStream(ValueType valueType, List<GraphStoreGraphPropertyVisitor.StreamBuilder<?>> streamBuilders) {
        switch (valueType) {
            case LONG:
                return new LongStreamBuilderGraphPropertyValues(streamBuilders);
            case DOUBLE:
                return new DoubleStreamBuilderGraphPropertyValues(streamBuilders);
            case FLOAT_ARRAY:
                return new FloatArrayStreamBuilderGraphPropertyValues(streamBuilders);
            case DOUBLE_ARRAY:
                return new DoubleArrayStreamBuilderGraphPropertyValues(streamBuilders);
            case LONG_ARRAY:
                return new LongArrayStreamBuilderGraphPropertyValues(streamBuilders);
            default:
                throw new UnsupportedOperationException();
        }
    }

    static class LongStreamBuilderGraphPropertyValues implements LongGraphPropertyValues {

        private final Collection<GraphStoreGraphPropertyVisitor.StreamBuilder<?>> streamFractions;

        LongStreamBuilderGraphPropertyValues(Collection<GraphStoreGraphPropertyVisitor.StreamBuilder<?>> streamFractions) {
            this.streamFractions = streamFractions;
        }

        @Override
        public long valueCount() {
            return -1;
        }

        @Override
        public LongStream longValues() {
            return (LongStream) mergeStreamFractions(streamFractions).stream();
        }
    }

    static class DoubleStreamBuilderGraphPropertyValues implements DoubleGraphPropertyValues {

        private final Collection<GraphStoreGraphPropertyVisitor.StreamBuilder<?>> streamFractions;

        DoubleStreamBuilderGraphPropertyValues(Collection<GraphStoreGraphPropertyVisitor.StreamBuilder<?>> streamFractions) {
            this.streamFractions = streamFractions;
        }

        @Override
        public long valueCount() {
            return -1;
        }

        @Override
        public DoubleStream doubleValues() {
            return (DoubleStream) mergeStreamFractions(streamFractions).stream();
        }
    }


    static class FloatArrayStreamBuilderGraphPropertyValues implements FloatArrayGraphPropertyValues {

        private final Collection<GraphStoreGraphPropertyVisitor.StreamBuilder<?>> streamFractions;

        FloatArrayStreamBuilderGraphPropertyValues(Collection<GraphStoreGraphPropertyVisitor.StreamBuilder<?>> streamFractions) {
            this.streamFractions = streamFractions;
        }

        @Override
        public long valueCount() {
            return -1;
        }

        @Override
        public Stream<float[]> floatArrayValues() {
            return (Stream<float[]>) mergeStreamFractions(streamFractions).stream();
        }
    }

    static class DoubleArrayStreamBuilderGraphPropertyValues implements DoubleArrayGraphPropertyValues {

        private final Collection<GraphStoreGraphPropertyVisitor.StreamBuilder<?>> streamFractions;

        DoubleArrayStreamBuilderGraphPropertyValues(Collection<GraphStoreGraphPropertyVisitor.StreamBuilder<?>> streamFractions) {
            this.streamFractions = streamFractions;
        }

        @Override
        public long valueCount() {
            return -1;
        }

        @Override
        public Stream<double[]> doubleArrayValues() {
            return (Stream<double[]>) mergeStreamFractions(streamFractions).stream();
        }
    }

    static class LongArrayStreamBuilderGraphPropertyValues implements LongArrayGraphPropertyValues {

        private final Collection<GraphStoreGraphPropertyVisitor.StreamBuilder<?>> streamFractions;

        LongArrayStreamBuilderGraphPropertyValues(Collection<GraphStoreGraphPropertyVisitor.StreamBuilder<?>> streamFractions) {
            this.streamFractions = streamFractions;
        }

        @Override
        public long valueCount() {
            return -1;
        }

        @Override
        public Stream<long[]> longArrayValues() {
            return (Stream<long[]>) mergeStreamFractions(streamFractions).stream();
        }
    }
}
