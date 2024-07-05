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
package org.neo4j.gds.applications.algorithms.embeddings;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.ResultStore;
import org.neo4j.gds.applications.algorithms.machinery.MutateOrWriteStep;
import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.core.utils.progress.JobId;
import org.neo4j.gds.embeddings.graphsage.GraphSageModelTrainer;
import org.neo4j.gds.embeddings.graphsage.ModelData;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageTrainConfig;

/**
 * GraphSage train mode wants to store the trained model in memory, and optionally write it to disk.
 * The {@link org.neo4j.gds.core.model.ModelCatalog} takes care of the former.
 * The {@link GraphSageModelRepository} takes care of the latter.
 */
class GraphSageTrainWriteToDiskStep implements MutateOrWriteStep<Model<ModelData, GraphSageTrainConfig, GraphSageModelTrainer.GraphSageTrainMetrics>, Void> {
    private final GraphSageModelCatalog graphSageModelCatalog;
    private final GraphSageModelRepository graphSageModelRepository;
    private final GraphSageTrainConfig configuration;

    GraphSageTrainWriteToDiskStep(
        GraphSageModelCatalog graphSageModelCatalog,
        GraphSageModelRepository graphSageModelRepository,
        GraphSageTrainConfig configuration
    ) {
        this.graphSageModelCatalog = graphSageModelCatalog;
        this.graphSageModelRepository = graphSageModelRepository;
        this.configuration = configuration;
    }

    @Override
    public Void execute(
        Graph graph,
        GraphStore graphStore,
        ResultStore resultStore,
        Model<ModelData, GraphSageTrainConfig, GraphSageModelTrainer.GraphSageTrainMetrics> result,
        JobId jobId
    ) {
        graphSageModelCatalog.store(result);

        if (configuration.storeModelToDisk()) graphSageModelRepository.store(result);

        return null;
    }
}
