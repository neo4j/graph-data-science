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
import org.neo4j.gds.applications.algorithms.machinery.WriteStep;
import org.neo4j.gds.applications.modelcatalog.ModelRepository;
import org.neo4j.gds.core.JobId;
import org.neo4j.gds.core.model.CustomInfo;
import org.neo4j.gds.core.model.TypedModel;
import org.neo4j.gds.embeddings.graphsage.ModelData;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageTrainConfig;

/**
 * GraphSage train mode wants to store the trained model in memory, and optionally write it to disk.
 * The {@link org.neo4j.gds.core.model.ModelCatalog} takes care of the former.
 * The {@link ModelRepository} takes care of the latter.
 */
class GraphSageTrainWriteToDiskStep implements WriteStep<TypedModel<ModelData, GraphSageTrainConfig, CustomInfo>, Void> {
    private final GraphSageModelCatalog graphSageModelCatalog;
    private final ModelRepository modelRepository;
    private final GraphSageTrainConfig configuration;

    GraphSageTrainWriteToDiskStep(
        GraphSageModelCatalog graphSageModelCatalog,
        ModelRepository modelRepository,
        GraphSageTrainConfig configuration
    ) {
        this.graphSageModelCatalog = graphSageModelCatalog;
        this.modelRepository = modelRepository;
        this.configuration = configuration;
    }

    @Override
    public Void execute(
        Graph graph,
        GraphStore graphStore,
        ResultStore resultStore,
        TypedModel<ModelData, GraphSageTrainConfig, CustomInfo> result,
        JobId jobId
    ) {
        graphSageModelCatalog.store(result);

        if (configuration.storeModelToDisk()) modelRepository.store(result.model());

        return null;
    }
}
