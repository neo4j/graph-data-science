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

import org.jetbrains.annotations.NotNull;
import org.neo4j.gds.api.properties.graph.DoubleArrayGraphPropertyValues;
import org.neo4j.gds.api.properties.graph.DoubleGraphPropertyValues;
import org.neo4j.gds.api.properties.graph.FloatArrayGraphPropertyValues;
import org.neo4j.gds.api.properties.graph.GraphProperty;
import org.neo4j.gds.api.properties.graph.GraphPropertyStore;
import org.neo4j.gds.api.properties.graph.GraphPropertyValues;
import org.neo4j.gds.api.properties.graph.LongArrayGraphPropertyValues;
import org.neo4j.gds.api.properties.graph.LongGraphPropertyValues;
import org.neo4j.gds.core.io.GraphStoreGraphPropertyVisitor;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

final class GraphPropertyStoreFromVisitorHelper {

    private GraphPropertyStoreFromVisitorHelper() {}

    static GraphPropertyStore fromGraphPropertyVisitor(GraphStoreGraphPropertyVisitor graphStoreGraphPropertyVisitor) {
        var graphPropertyBuilder = GraphPropertyStore.builder();
        var graphPropertyStreams = mergeStreamFractions(graphStoreGraphPropertyVisitor.streamFractions());
        buildGraphPropertiesFromStreams(graphPropertyBuilder, graphPropertyStreams);

        return graphPropertyBuilder.build();
    }

    private static void buildGraphPropertiesFromStreams(
        GraphPropertyStore.Builder graphPropertyBuilder,
        Map<String, Optional<? extends GraphStoreGraphPropertyVisitor.ReducibleStream<?>>> graphPropertyStreams
    ) {
        graphPropertyStreams.forEach((key, value) -> {
            if (value.isPresent()) {
                var graphPropertyValues = getGraphPropertyValuesFromStream(value.get());
                graphPropertyBuilder.putProperty(key, GraphProperty.of(key, graphPropertyValues));
            }
        });
    }

    @NotNull
    private static Map<String, Optional<? extends GraphStoreGraphPropertyVisitor.ReducibleStream<?>>> mergeStreamFractions(Map<String, List<GraphStoreGraphPropertyVisitor.StreamBuilder<?>>> streamFractions) {
        return streamFractions.entrySet().stream().collect(Collectors.toMap(
            Map.Entry::getKey,
            entry -> entry.getValue()
                .stream()
                .map(GraphStoreGraphPropertyVisitor.StreamBuilder::build)
                .reduce(GraphStoreGraphPropertyVisitor.ReducibleStream::reduce)
        ));
    }

    private static GraphPropertyValues getGraphPropertyValuesFromStream(GraphStoreGraphPropertyVisitor.ReducibleStream<?> reducibleStream) {
        switch (reducibleStream.valueType()) {
            case LONG:
                return LongGraphPropertyValues.ofLongStream((LongStream) reducibleStream.stream());
            case DOUBLE:
                return DoubleGraphPropertyValues.ofDoubleStream((DoubleStream) reducibleStream.stream());
            case FLOAT_ARRAY:
                return FloatArrayGraphPropertyValues.ofFloatArrayStream((Stream<float[]>) reducibleStream.stream());
            case DOUBLE_ARRAY:
                return DoubleArrayGraphPropertyValues.ofDoubleArrayStream((Stream<double[]>) reducibleStream.stream());
            case LONG_ARRAY:
                return LongArrayGraphPropertyValues.ofLongArrayStream((Stream<long[]>) reducibleStream.stream());
            default:
                throw new UnsupportedOperationException();
        }
    }
}
