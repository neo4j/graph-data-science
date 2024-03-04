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
import org.neo4j.gds.collections.ha.HugeObjectArray;
import org.neo4j.gds.config.MutateConfig;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.embeddings.graphsage.GraphSageHelper;

import static org.neo4j.gds.core.utils.mem.MemoryEstimations.RESIDENT_MEMORY;
import static org.neo4j.gds.core.utils.mem.MemoryEstimations.TEMPORARY_MEMORY;
import static org.neo4j.gds.embeddings.graphsage.algo.GraphSageModelResolver.resolveModel;
import static org.neo4j.gds.mem.MemoryUsage.sizeOfDoubleArray;
import static org.neo4j.gds.ml.core.EmbeddingUtils.validateRelationshipWeightPropertyValue;

public class GraphSageAlgorithmFactory<CONFIG extends GraphSageBaseConfig> extends GraphAlgorithmFactory<GraphSage, CONFIG> {

    private final ModelCatalog modelCatalog;

    public GraphSageAlgorithmFactory(ModelCatalog modelCatalog) {
        super();
        this.modelCatalog = modelCatalog;
    }


    @Override
    public GraphSage build(Graph graph, CONFIG configuration, ProgressTracker progressTracker) {
        var executorService = DefaultPool.INSTANCE;
        var model = resolveModel(
            modelCatalog,
            configuration.modelUser(),
            configuration.modelName()
        );


        if (graph.hasRelationshipProperty()) {
            validateRelationshipWeightPropertyValue(graph, configuration.concurrency(), executorService);
        }

        return new GraphSage(
            graph,
            model,
            configuration,
            executorService,
            progressTracker
        );
    }

    @Override
    public String taskName() {
        return GraphSage.class.getSimpleName();
    }

    @Override
    public MemoryEstimation memoryEstimation(CONFIG config) {
        var model = resolveModel(modelCatalog, config.username(), config.modelName());

        return MemoryEstimations.setup(
            "",
            graphDimensions -> withNodeCount(
                model.trainConfig(),
                graphDimensions.nodeCount(),
                config instanceof MutateConfig
            )
        );
    }

    @Override
    public Task progressTask(Graph graph, CONFIG config) {
        return Tasks.leaf(taskName(), graph.nodeCount());
    }

    private MemoryEstimation withNodeCount(GraphSageTrainConfig config, long nodeCount, boolean mutate) {
        var estimationParameters = config.toMemoryEstimateParameters();
        var gsBuilder = MemoryEstimations.builder("GraphSage");

        if (mutate) {
            gsBuilder = gsBuilder
                .startField(RESIDENT_MEMORY)
                .perNode(
                    "resultFeatures",
                    nc -> HugeObjectArray.memoryEstimation(nc, sizeOfDoubleArray(estimationParameters.embeddingDimension()))
                )
                .endField();
        }

        var builder = gsBuilder
            .startField(TEMPORARY_MEMORY)
            .field("this.instance", GraphSage.class)
            .perNode(
                "initialFeatures",
                nc -> HugeObjectArray.memoryEstimation(nc, sizeOfDoubleArray(estimationParameters.estimationFeatureDimension()))
            )
            .perThread(
                "concurrentBatches",
                MemoryEstimations.builder().add(
                    GraphSageHelper.embeddingsEstimation(estimationParameters, estimationParameters.batchSize(), nodeCount, 0, false)
                ).build()
            );
        if (!mutate) {
            builder = builder.perNode(
                "resultFeatures",
                nc -> HugeObjectArray.memoryEstimation(nc, sizeOfDoubleArray(estimationParameters.embeddingDimension()))
            );
        }
        return builder.endField().build();
    }


}
