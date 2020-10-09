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
package org.neo4j.gds.embeddings.graphsage.proc;

import org.neo4j.gds.embeddings.graphsage.Aggregator;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSage;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageTrain;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageTrainConfig;
import org.neo4j.graphalgo.AlgorithmFactory;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;
import org.neo4j.graphalgo.core.utils.mem.MemoryRange;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;
import org.neo4j.logging.Log;

import java.util.ArrayList;

import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.sizeOfDoubleArray;
import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.sizeOfIntArray;
import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.sizeOfLongArray;
import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.sizeOfObjectArray;

public final class GraphSageTrainAlgorithmFactory implements AlgorithmFactory<GraphSageTrain, GraphSageTrainConfig> {

    @Override
    public GraphSageTrain build(
        Graph graph,
        GraphSageTrainConfig configuration,
        AllocationTracker tracker,
        Log log
    ) {
        return new GraphSageTrain(graph, configuration, tracker, log);
    }

    @Override
    public MemoryEstimation memoryEstimation(GraphSageTrainConfig configuration) {
        return MemoryEstimations.setup(
            "",
            graphDimensions -> withNodeCount(configuration, graphDimensions.nodeCount())
        );
    }

    private MemoryEstimation withNodeCount(GraphSageTrainConfig config, long nodeCount) {
        var layerConfigs = config.layerConfigs();
        var numberOfLayers = layerConfigs.size();

        var rootBuilder = MemoryEstimations.builder(GraphSage.class);
        var layerBuilder = rootBuilder
            .startField("persistentMemory")
            .startField("layers");

        var trainOnBatchBuilder = MemoryEstimations.builder().startField("trainOnBatch");
        var lossBuilder = trainOnBatchBuilder.startField("lossFunction");
        var subGraphBuilder = lossBuilder.startField("subgraphs");

        final var minBatchNodeCounts = new ArrayList<Long>(numberOfLayers + 1);
        final var maxBatchNodeCounts = new ArrayList<Long>(numberOfLayers + 1);
        minBatchNodeCounts.add(3L * config.batchSize());
        maxBatchNodeCounts.add(3L * config.batchSize());

        long initialAdamOptimizer = 0L;
        long updateAdamOptimizer = 0L;
        for (int i = 0; i < numberOfLayers; i++) {
            var layerConfig = layerConfigs.get(i);
            var weightDimensions = layerConfig.rows() * layerConfig.cols();
            var weightsMemory = sizeOfDoubleArray(weightDimensions);
            if (layerConfig.aggregatorType() == Aggregator.AggregatorType.POOL) {
                // selfWeights
                weightsMemory += sizeOfDoubleArray(layerConfig.rows() * layerConfig.rows());
                // neighborsWeights
                weightsMemory += sizeOfDoubleArray(layerConfig.rows() * layerConfig.rows());
                // bias
                weightsMemory += sizeOfDoubleArray(layerConfig.rows());
            }
            layerBuilder.fixed("layer " + (i + 1), weightsMemory);

            initialAdamOptimizer += 2 * sizeOfDoubleArray(weightDimensions);
            updateAdamOptimizer += 5 * weightDimensions;

            var sampleSize = layerConfig.sampleSize();

            var min = minBatchNodeCounts.get(i);
            var max = maxBatchNodeCounts.get(i);
            var minNextNodeCount = Math.min(min, nodeCount);
            var maxNextNodeCount = Math.min(max * (sampleSize + 1), nodeCount);
            minBatchNodeCounts.add(minNextNodeCount);
            maxBatchNodeCounts.add(maxNextNodeCount);

            var subgraphRange = MemoryRange.of(
                sizeOfIntArray(min) + sizeOfObjectArray(min) + min * sizeOfIntArray(0) + sizeOfLongArray(minNextNodeCount),
                sizeOfIntArray(max) + sizeOfObjectArray(max) + max * sizeOfIntArray(sampleSize) + sizeOfLongArray(maxNextNodeCount)
            );

            subGraphBuilder.add(MemoryEstimations.of("subgraph " + (i + 1), subgraphRange));
        }
        subGraphBuilder.endField();
        layerBuilder.endField().endField();

        var previousLayerMinNodeCounts = new ArrayList<>(minBatchNodeCounts);
        previousLayerMinNodeCounts.set(
            minBatchNodeCounts.size() - 1,
            minBatchNodeCounts.get(minBatchNodeCounts.size() - 2)
        );
        var previousLayerMaxNodeCounts = new ArrayList<>(maxBatchNodeCounts);
        previousLayerMaxNodeCounts.set(
            maxBatchNodeCounts.size() - 1,
            maxBatchNodeCounts.get(maxBatchNodeCounts.size() - 2)
        );

        var aggregatorsBuilder = lossBuilder.startField("aggregators");
        for (int i = 0; i < numberOfLayers; i++) {
            var layerConfig = layerConfigs.get(i);
            // aggregators go backwards through the layers
            var minNodeCount = minBatchNodeCounts.get(numberOfLayers - i - 1);
            var maxNodeCount = maxBatchNodeCounts.get(numberOfLayers - i - 1);

            if (i == 0) {
                aggregatorsBuilder.fixed(
                    "firstLayer",
                    MemoryRange.of(sizeOfDoubleArray(minNodeCount), sizeOfDoubleArray(maxNodeCount))
                );
            }

            Aggregator.AggregatorType aggregatorType = layerConfig.aggregatorType();
            if (aggregatorType == Aggregator.AggregatorType.MEAN) {
                var minBound =
                    sizeOfDoubleArray(minNodeCount * layerConfig.cols()) +
                    2 * sizeOfDoubleArray(minNodeCount * config.embeddingDimension());
                var maxBound =
                    sizeOfDoubleArray(maxNodeCount * layerConfig.cols()) +
                    2 * sizeOfDoubleArray(maxNodeCount * config.embeddingDimension());

                aggregatorsBuilder.add("MEAN " + (i + 1), MemoryEstimations.of("", MemoryRange.of(
                    minBound,
                    maxBound
                )));
            } else if (aggregatorType == Aggregator.AggregatorType.POOL) {
                var minPreviousNodeCount = previousLayerMinNodeCounts.get(numberOfLayers - i);
                var maxPreviousNodeCount = previousLayerMaxNodeCounts.get(numberOfLayers - i);

                var minBound =
                    3 * sizeOfDoubleArray(minPreviousNodeCount * config.embeddingDimension()) +
                    6 * sizeOfDoubleArray(minNodeCount * config.embeddingDimension());
                var maxBound =
                    3 * sizeOfDoubleArray(maxPreviousNodeCount * config.embeddingDimension()) +
                    6 * sizeOfDoubleArray(maxNodeCount * config.embeddingDimension());

                aggregatorsBuilder.add(
                    "POOL " + (i + 1),
                    MemoryEstimations.of("", MemoryRange.of(minBound, maxBound))
                );
            }

            if (i == numberOfLayers - 1) {
                aggregatorsBuilder.fixed(
                    "normalizeRows",
                    MemoryRange.of(
                        sizeOfDoubleArray(minNodeCount * config.embeddingDimension()),
                        sizeOfDoubleArray(maxNodeCount * config.embeddingDimension())
                    )
                );
            }
        }
        var gradientDescentAggregators = aggregatorsBuilder.build();
        aggregatorsBuilder.endField();
        lossBuilder.endField();
        trainOnBatchBuilder.add("gradientDescent", gradientDescentAggregators);

        rootBuilder.startField("peakMemory")
            .add("initialFeatures", HugeObjectArray.memoryEstimation(sizeOfDoubleArray(config.featuresSize())))
            .startField("trainOnEpoch")
            .fixed("initialAdamOptimizer", initialAdamOptimizer)
            .perThread("concurrentBatches", trainOnBatchBuilder
                .fixed("updateAdamOptimizer", updateAdamOptimizer)
                .endField()
                .build())
            .endField()
            .endField();

        return rootBuilder.build();
    }
}
