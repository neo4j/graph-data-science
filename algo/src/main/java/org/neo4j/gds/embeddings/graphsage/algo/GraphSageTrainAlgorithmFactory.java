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
import org.neo4j.gds.compat.GdsVersionInfoProvider;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.embeddings.graphsage.GraphSageModelTrainer;

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
        var executorService = DefaultPool.INSTANCE;
        var gdsVersion = GdsVersionInfoProvider.GDS_VERSION_INFO.gdsVersion();

        if(configuration.hasRelationshipWeightProperty()) {
            validateRelationshipWeightPropertyValue(graph, configuration.typedConcurrency(), executorService);
        }

        return configuration.isMultiLabel()
        ? new MultiLabelGraphSageTrain(graph, configuration.toParameters(), configuration.projectedFeatureDimension().get(), executorService, progressTracker, gdsVersion, configuration)
        : new SingleLabelGraphSageTrain(graph, configuration.toParameters(), executorService, progressTracker, gdsVersion, configuration);
    }

    public MemoryEstimation memoryEstimation(GraphSageTrainMemoryEstimateParameters parameters) {
        return new GraphSageTrainEstimateDefinition(parameters).memoryEstimation();
    }

    @Override
    public MemoryEstimation memoryEstimation(GraphSageTrainConfig configuration) {
        return memoryEstimation(configuration.toMemoryEstimateParameters());
    }

    public Task progressTask(long nodeCount, GraphSageTrainParameters parameters) {
        return Tasks.task(
            taskName(),
            GraphSageModelTrainer.progressTasks(
                parameters.numberOfBatches(nodeCount),
                parameters.batchesPerIteration(nodeCount),
                parameters.maxIterations(),
                parameters.epochs()
            )
        );
    }

    @Override
    public Task progressTask(Graph graph, GraphSageTrainConfig config) {
        return progressTask(graph.nodeCount(), config.toParameters());
    }

}
