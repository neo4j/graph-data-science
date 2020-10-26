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
import org.neo4j.gds.embeddings.graphsage.ddl4j.Variable;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.MatrixConstant;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.NormalizeRows;
import org.neo4j.gds.embeddings.graphsage.ddl4j.tensor.Matrix;
import org.neo4j.gds.embeddings.graphsage.subgraph.SubGraph;
import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;
import org.neo4j.graphalgo.core.utils.mem.MemoryRange;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;
import static org.neo4j.gds.embeddings.EmbeddingUtils.getCheckedDoubleNodeProperty;
import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.sizeOfDoubleArray;
import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.sizeOfIntArray;
import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.sizeOfLongArray;
import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.sizeOfObjectArray;
import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

public final class GraphSageHelper {

    private GraphSageHelper() {}

    public static Variable<Matrix> embeddings(
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
                    subGraphs.get(layerNr)
                );
        }
        return new NormalizeRows(previousLayerRepresentations);
    }

    public static MemoryEstimation embeddingsEstimation(
        GraphSageTrainConfig config,
        long batchSize,
        long nodeCount,
        int labelCount,
        boolean withGradientDescent
    ) {
        var isMultiLabel = config.isMultiLabel();

        var layerConfigs = config.layerConfigs();
        var numberOfLayers = layerConfigs.size();

        var computationGraphBuilder = MemoryEstimations.builder("computationGraph").startField("subgraphs");

        final var minBatchNodeCounts = new ArrayList<Long>(numberOfLayers + 1);
        final var maxBatchNodeCounts = new ArrayList<Long>(numberOfLayers + 1);
        minBatchNodeCounts.add(batchSize);
        maxBatchNodeCounts.add(batchSize);

        for (int i = 0; i < numberOfLayers; i++) {
            var sampleSize = layerConfigs.get(i).sampleSize();

            var min = minBatchNodeCounts.get(i);
            var max = maxBatchNodeCounts.get(i);
            var minNextNodeCount = Math.min(min, nodeCount);
            var maxNextNodeCount = Math.min(max * (sampleSize + 1), nodeCount);
            minBatchNodeCounts.add(minNextNodeCount);
            maxBatchNodeCounts.add(maxNextNodeCount);

            var subgraphRange = MemoryRange.of(
                sizeOfIntArray(min) + sizeOfObjectArray(min) + min * sizeOfIntArray(0) + sizeOfLongArray(
                    minNextNodeCount),
                sizeOfIntArray(max) + sizeOfObjectArray(max) + max * sizeOfIntArray(sampleSize) + sizeOfLongArray(
                    maxNextNodeCount)
            );

            computationGraphBuilder.add(MemoryEstimations.of("subgraph " + (i + 1), subgraphRange));
        }

        // aggregators go backwards through the layers
        Collections.reverse(minBatchNodeCounts);
        Collections.reverse(maxBatchNodeCounts);

        var aggregatorsBuilder = MemoryEstimations.builder();
        for (int i = 0; i < numberOfLayers; i++) {
            var layerConfig = layerConfigs.get(i);

            var minPreviousNodeCount = minBatchNodeCounts.get(i);
            var maxPreviousNodeCount = maxBatchNodeCounts.get(i);
            var minNodeCount = minBatchNodeCounts.get(i + 1);
            var maxNodeCount = maxBatchNodeCounts.get(i + 1);

            if (i == 0) {
                var featureSize = config.nodePropertyNames().size() + (config.degreeAsProperty() ? 1 : 0);
                MemoryRange firstLayerMemory = MemoryRange.of(
                    sizeOfDoubleArray(minPreviousNodeCount * featureSize),
                    sizeOfDoubleArray(maxPreviousNodeCount * featureSize)
                );
                if (isMultiLabel) {
                    firstLayerMemory = firstLayerMemory.add(MemoryRange.of(sizeOfDoubleArray(config.projectedFeatureDimension())));
                }
                aggregatorsBuilder.fixed("firstLayer", firstLayerMemory);
            }

            Aggregator.AggregatorType aggregatorType = layerConfig.aggregatorType();
            var embeddingDimension = config.embeddingDimension();

            aggregatorsBuilder.fixed(
                formatWithLocale("%s %d", aggregatorType.name(), i + 1),
                aggregatorType.memoryEstimation(
                    minNodeCount,
                    maxNodeCount,
                    minPreviousNodeCount,
                    maxPreviousNodeCount,
                    layerConfig.cols(),
                    embeddingDimension
                )
            );

            if (i == numberOfLayers - 1) {
                aggregatorsBuilder.fixed(
                    "normalizeRows",
                    MemoryRange.of(
                        sizeOfDoubleArray(minNodeCount * embeddingDimension),
                        sizeOfDoubleArray(maxNodeCount * embeddingDimension)
                    )
                );
            }
        }

        computationGraphBuilder = computationGraphBuilder
            .endField();

        if (isMultiLabel) {
            var minFeatureFunction = sizeOfObjectArray(minBatchNodeCounts.get(0));
            var maxFeatureFunction = sizeOfObjectArray(maxBatchNodeCounts.get(0));
            var copyOfLabels = sizeOfObjectArray(labelCount);

            computationGraphBuilder.fixed(
                "multiLabelFeatureFunction",
                MemoryRange.of(minFeatureFunction, maxFeatureFunction).add(MemoryRange.of(copyOfLabels))
            );
        }

        computationGraphBuilder = computationGraphBuilder
            .startField("forward")
            .addComponentsOf(aggregatorsBuilder.build());

        if (withGradientDescent) {
            computationGraphBuilder = computationGraphBuilder
                .endField()
                .startField("backward")
                .addComponentsOf(aggregatorsBuilder.build());
        }
        return computationGraphBuilder.endField().build();
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

        if (config.isMultiLabel()) {
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

        var featureCount = nodeProperties.size() + (config.degreeAsProperty() ? 1 : 0);

        features.setAll(nodeId -> {
            var nodeFeatures = new double[featureCount];

            for (int i = 0; i < nodeProperties.size(); i++) {
                double doubleValue = getCheckedDoubleNodeProperty(graph, config.nodePropertyNames().get(i), nodeId);
                nodeFeatures[i] = doubleValue;
            }

            if (config.degreeAsProperty()) {
                nodeFeatures[featureCount - 1] = graph.degree(nodeId);
            }

            return nodeFeatures;
        });

        return features;
    }

    private static HugeObjectArray<double[]> initializeMultiLabelFeatures(
        Graph graph,
        GraphSageTrainConfig config,
        HugeObjectArray<double[]> features
    ) {
        var filteredKeysPerLabel = filteredPropertyKeysPerNodeLabel(graph, config);
        var featureCountPerNodeLabel = filteredKeysPerLabel.entrySet().stream()
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> entry.getValue().size()
                         + (config.degreeAsProperty() ? 1 : 0)
                         + 1 // Label is used as a property
            ));

        features.setAll(nodeId -> {
            var nodeLabel = labelOf(graph, nodeId);
            var filteredKeys = filteredKeysPerLabel.get(nodeLabel);
            var featureCount = featureCountPerNodeLabel.get(nodeLabel);
            var nodeFeatures = new double[featureCount];

            int i = 0;
            for (String propertyKey : filteredKeys) {
                nodeFeatures[i++] = getCheckedDoubleNodeProperty(graph, propertyKey, nodeId);
            }

            if (config.degreeAsProperty()) {
                nodeFeatures[i++] = graph.degree(nodeId);
            }
            // Label is used as a property
            nodeFeatures[i] = 1.0;

            return nodeFeatures;
        });
        return features;
    }

    public static Variable<Matrix> features(long[] nodeIds, HugeObjectArray<double[]> features) {
        int dimension = features.get(0).length;
        double[] data = new double[Math.multiplyExact(nodeIds.length, dimension)];
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
        return graph.schema()
            .nodeSchema()
            .properties()
            .entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().keySet()));
    }

    private static Map<NodeLabel, Set<String>> filteredPropertyKeysPerNodeLabel(Graph graph, GraphSageTrainConfig config) {
        return propertyKeysPerNodeLabel(graph)
            .entrySet()
            .stream()
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                e -> config.nodePropertyNames()
                    .stream()
                    .filter(e.getValue()::contains)
                    .collect(Collectors.toSet())
            ));
    }

    private static NodeLabel labelOf(Graph graph, long n) {
        var nodeLabels = graph.nodeLabels(n);
        if (nodeLabels.size() != 1) {
            throw new IllegalArgumentException(
                formatWithLocale("Each node must have exactly one label: nodeId=%d, labels=%s", n, nodeLabels)
            );
        }
        return graph.nodeLabels(n).iterator().next();
    }
}
