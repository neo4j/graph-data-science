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
package org.neo4j.gds.embeddings.graphsage.algo;

import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.embeddings.graphsage.Aggregator;
import org.neo4j.gds.embeddings.graphsage.GraphSageHelper;
import org.neo4j.gds.embeddings.graphsage.GraphSageModelTrainer;

import static org.neo4j.gds.core.utils.mem.MemoryEstimations.RESIDENT_MEMORY;
import static org.neo4j.gds.core.utils.mem.MemoryEstimations.TEMPORARY_MEMORY;
import static org.neo4j.gds.mem.MemoryUsage.sizeOfDoubleArray;
import static org.neo4j.gds.ml.core.EmbeddingUtils.validateRelationshipWeightPropertyValue;

public final class GraphSageTrainAlgorithmFactory extends GraphAlgorithmFactory<GraphSageTrain, GraphSageTrainConfig> {

    public GraphSageTrainAlgorithmFactory() {
        super();
    }

    @Override
    public String taskName() {
        return GraphSageTrain.class.getSimpleName();
    }

    @Override
    public GraphSageTrain build(
        Graph graph,
        GraphSageTrainConfig configuration,
        ProgressTracker progressTracker
    ) {
        var executorService = Pools.DEFAULT;
        if(configuration.hasRelationshipWeightProperty()) {
            validateRelationshipWeightPropertyValue(graph, configuration.concurrency(), executorService);
        }

        return configuration.isMultiLabel()
        ? new MultiLabelGraphSageTrain(graph, configuration, executorService, progressTracker)
        : new SingleLabelGraphSageTrain(graph, configuration, executorService, progressTracker);
    }

    @Override
    public MemoryEstimation memoryEstimation(GraphSageTrainConfig configuration) {
        return MemoryEstimations.setup(
            "",
            graphDimensions -> estimate(
                configuration,
                graphDimensions.nodeCount(),
                graphDimensions.estimationNodeLabelCount()
            )
        );
    }

    @Override
    public Task progressTask(Graph graph, GraphSageTrainConfig config) {
        return Tasks.task(taskName(), GraphSageModelTrainer.progressTasks(config));
    }

    private MemoryEstimation estimate(GraphSageTrainConfig config, long nodeCount, int labelCount) {
        var layerConfigs = config.layerConfigs(config.estimationFeatureDimension());
        var numberOfLayers = layerConfigs.size();

        var layerBuilder = MemoryEstimations.builder("GraphSageTrain")
            .startField(RESIDENT_MEMORY)
            .startField("weights");

        long initialAdamOptimizer = 0L;
        long updateAdamOptimizer = 0L;
        for (int i = 0; i < numberOfLayers; i++) {
            var layerConfig = layerConfigs.get(i);
            var weightDimensions = layerConfig.rows() * layerConfig.cols();
            var weightsMemory = sizeOfDoubleArray(weightDimensions);
            if (layerConfig.aggregatorType() == Aggregator.AggregatorType.POOL) {
                // selfWeights
                weightsMemory += sizeOfDoubleArray((long) layerConfig.rows() * layerConfig.rows());
                // neighborsWeights
                weightsMemory += sizeOfDoubleArray((long) layerConfig.rows() * layerConfig.rows());
                // bias
                weightsMemory += sizeOfDoubleArray(layerConfig.rows());
            }
            layerBuilder.fixed("layer " + (i + 1), weightsMemory);

            initialAdamOptimizer += 2 * sizeOfDoubleArray(weightDimensions);
            updateAdamOptimizer += 5L * weightDimensions;
        }

        var isMultiLabel = config.isMultiLabel();

        var perNodeFeaturesMemory = MemoryRange.of(
            sizeOfDoubleArray(isMultiLabel ? 1 : config.estimationFeatureDimension()),
            sizeOfDoubleArray(config.estimationFeatureDimension())
        );
        var initialFeaturesMemory = HugeObjectArray.memoryEstimation(MemoryEstimations.of("", perNodeFeaturesMemory));

        var estimationsBuilder = layerBuilder
            .endField()
            .endField()
            .startField(TEMPORARY_MEMORY)
            .field("this.instance", GraphSage.class);

        if (isMultiLabel) {
            var minNumProperties = 1;
            var maxNumProperties = config.featureProperties().size();
            maxNumProperties++; // Add one for the label
            var minWeightsMemory = sizeOfDoubleArray(config.estimationFeatureDimension() * minNumProperties);
            var maxWeightsMemory = sizeOfDoubleArray((long) config.estimationFeatureDimension() * maxNumProperties);
            var weightByLabelMemory = MemoryRange.of(minWeightsMemory, maxWeightsMemory).times(labelCount);

            estimationsBuilder.fixed("weightsByLabel", weightByLabelMemory);
        }

        return estimationsBuilder
            .add("initialFeatures", initialFeaturesMemory)
            .startField("trainOnEpoch")
            .fixed("initialAdamOptimizer", initialAdamOptimizer)
            .perThread("concurrentBatches", MemoryEstimations
                .builder()
                .startField("trainOnBatch")
                .add(GraphSageHelper.embeddingsEstimation(config, 3L * config.batchSize(), nodeCount, labelCount, true))
                .fixed("updateAdamOptimizer", updateAdamOptimizer)
                .endField()
                .build())
            .endField()
            .endField()
            .build();
    }
}
