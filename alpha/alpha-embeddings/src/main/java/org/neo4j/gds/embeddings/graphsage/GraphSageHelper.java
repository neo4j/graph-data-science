/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.gds.embeddings.graphsage;

import org.neo4j.gds.embeddings.graphsage.algo.GraphSageTrainConfig;
import org.neo4j.gds.embeddings.graphsage.algo.MultiLabelGraphSageTrainConfig;
import org.neo4j.gds.embeddings.graphsage.ddl4j.Variable;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.MatrixConstant;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.NormalizeRows;
import org.neo4j.gds.embeddings.graphsage.ddl4j.tensor.Matrix;
import org.neo4j.gds.embeddings.graphsage.subgraph.SubGraph;
import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;

public final class GraphSageHelper {

    private GraphSageHelper() {}

    static Variable<Matrix> embeddings(
        Graph graph,
        long[] nodeIds,
        HugeObjectArray<double[]> features,
        Layer[] layers,
        FeatureFunction featureFunction
    ) {
        List<NeighborhoodFunction> neighborhoodFunctions = Arrays
            .stream(layers)
            .map(layer -> (NeighborhoodFunction) layer::neighborhoodFunction)
            .collect(Collectors.toList());
        Collections.reverse(neighborhoodFunctions);
        List<SubGraph> subGraphs = SubGraph.buildSubGraphs(nodeIds, neighborhoodFunctions, graph);

        Variable<Matrix> previousLayerRepresentations = featureFunction.apply(
            subGraphs.get(subGraphs.size() - 1).nextNodes,
            features
        );

        for (int layerNr = layers.length - 1; layerNr >= 0; layerNr--) {
            Layer layer = layers[layers.length - layerNr - 1];
            previousLayerRepresentations = layer
                .aggregator()
                .aggregate(
                    previousLayerRepresentations,
                    subGraphs.get(layerNr).adjacency,
                    subGraphs.get(layerNr).selfAdjacency
                );
        }
        return new NormalizeRows(previousLayerRepresentations);
    }

    public static HugeObjectArray<double[]> initializeFeatures(
        Graph graph,
        GraphSageTrainConfig config,
        AllocationTracker tracker
    ) {
        HugeObjectArray<double[]> features = HugeObjectArray.newArray(
            double[].class,
            graph.nodeCount(),
            tracker
        );

        if (config instanceof MultiLabelGraphSageTrainConfig) {
            return initializeMultiLabelFeatures(graph, config, features);
        } else {
            return initializeSingleLabelFeatures(graph, config, features);
        }
    }

    private static HugeObjectArray<double[]> initializeSingleLabelFeatures(
        Graph graph,
        GraphSageTrainConfig config,
        HugeObjectArray<double[]> features
    ) {
        var nodeProperties =
            config.nodePropertyNames()
                .stream()
                .map(graph::nodeProperties)
                .collect(toList());

        features.setAll(n -> {
            DoubleStream nodeFeatures = nodeProperties.stream().mapToDouble(p -> p.doubleValue(n));
            if (config.degreeAsProperty()) {
                nodeFeatures = DoubleStream.concat(nodeFeatures, DoubleStream.of(graph.degree(n)));
            }
            return nodeFeatures.toArray();
        });
        return features;
    }

    private static HugeObjectArray<double[]> initializeMultiLabelFeatures(
        Graph graph,
        GraphSageTrainConfig config,
        HugeObjectArray<double[]> features
    ) {
        var propertiesPerNodeLabel = propertiesPerNodeLabel(graph, config);
        features.setAll(n -> {
            var relevantProperties = propertiesPerNodeLabel.get(labelOf(graph, n));
            DoubleStream nodeFeatures = relevantProperties.stream().mapToDouble(p -> p.doubleValue(n));
            if (config.degreeAsProperty()) {
                nodeFeatures = DoubleStream.concat(nodeFeatures, DoubleStream.of(graph.degree(n)));
            }
            return nodeFeatures.toArray();
        });
        return features;
    }

    public static Variable<Matrix> features(long[] nodeIds, HugeObjectArray<double[]> features) {
        int dimension = features.get(0).length;
        double[] data = new double[nodeIds.length * dimension];
        IntStream
            .range(0, nodeIds.length)
            .forEach(nodeOffset -> System.arraycopy(
                features.get(nodeIds[nodeOffset]),
                0,
                data,
                nodeOffset * dimension,
                dimension
            ));
        return new MatrixConstant(data, nodeIds.length, dimension);
    }

    public static Map<NodeLabel, Set<String>> propertyKeysPerNodeLabel(Graph graph) {
        return graph.schema().nodeSchema().properties()
            .entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().keySet()));
    }

    private static Map<NodeLabel, Set<NodeProperties>> propertiesPerNodeLabel(Graph graph, GraphSageTrainConfig config) {
        return propertyKeysPerNodeLabel(graph)
            .entrySet()
            .stream()
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                e -> config.nodePropertyNames()
                    .stream()
                    .filter(e.getValue()::contains)
                    .map(graph::nodeProperties)
                    .collect(Collectors.toSet())
            ));
    }

    private static NodeLabel labelOf(Graph graph, long n) {
        return graph.nodeLabels(n).stream().findFirst().get();
    }
}
