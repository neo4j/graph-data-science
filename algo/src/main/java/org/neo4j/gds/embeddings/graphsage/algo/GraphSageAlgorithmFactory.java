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
import org.neo4j.gds.config.MutateConfig;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.embeddings.graphsage.TrainConfigTransformer;
import org.neo4j.gds.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.embeddings.graphsage.GraphSageModelTrainer.GraphSageTrainMetrics;
import org.neo4j.gds.embeddings.graphsage.ModelData;

import static org.neo4j.gds.embeddings.graphsage.algo.GraphSageModelResolver.resolveModel;
import static org.neo4j.gds.ml.core.EmbeddingUtils.validateRelationshipWeightPropertyValue;

public class GraphSageAlgorithmFactory<CONFIG extends GraphSageBaseConfig> extends GraphAlgorithmFactory<GraphSage, CONFIG> {

    private final ModelCatalog modelCatalog;

    public GraphSageAlgorithmFactory(ModelCatalog modelCatalog) {
        super();
        this.modelCatalog = modelCatalog;
    }

    public GraphSage build(
        Graph graph,
        GraphSageParameters parameters,
        Model<ModelData, GraphSageTrainConfig, GraphSageTrainMetrics> model,
        ProgressTracker progressTracker
    ) {
        var executorService = DefaultPool.INSTANCE;

        if (graph.hasRelationshipProperty()) {
            validateRelationshipWeightPropertyValue(graph, parameters.concurrency(), executorService);
        }

        return new GraphSage(
            graph,
            model,
            parameters.concurrency(),
            parameters.batchSize(),
            executorService,
            progressTracker
        );
    }

    @Override
    public GraphSage build(Graph graph, CONFIG configuration, ProgressTracker progressTracker) {
        var model = resolveModel(
            modelCatalog,
            configuration.modelUser(),
            configuration.modelName()
        );
        return build(graph, configuration.toParameters(), model, progressTracker);
    }

    @Override
    public String taskName() {
        return GraphSage.class.getSimpleName();
    }

    @Override
    public MemoryEstimation memoryEstimation(CONFIG config) {
        var model = resolveModel(modelCatalog, config.username(), config.modelName());

        return new GraphSageMemoryEstimateDefinition(
            TrainConfigTransformer.toMemoryEstimateParameters(model.trainConfig()),
            config instanceof MutateConfig
        ).memoryEstimation();
    }

    @Override
    public Task progressTask(Graph graph, CONFIG config) {
        return Tasks.leaf(taskName(), graph.nodeCount());
    }


}
