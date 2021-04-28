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
package org.neo4j.gds.embeddings.graphsage;

import org.jetbrains.annotations.NotNull;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSage;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageBaseConfig;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageTrainConfig;
import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.api.GraphStore;
import org.neo4j.graphalgo.api.GraphStoreValidation;
import org.neo4j.graphalgo.api.nodeproperties.DoubleArrayNodeProperties;
import org.neo4j.graphalgo.core.model.ModelCatalog;

public final class GraphSageCompanion {

    public static final String GRAPHSAGE_DESCRIPTION = "The GraphSage algorithm inductively computes embeddings for nodes based on a their features and neighborhoods.";

    private GraphSageCompanion() {}

    @NotNull
    public static <T extends GraphSageBaseConfig> DoubleArrayNodeProperties getNodeProperties(AlgoBaseProc.ComputationResult<GraphSage, GraphSage.GraphSageResult, T> computationResult) {
        return computationResult.result().embeddings()::get;
    }

    /**
     * Validate the train config that is stored on the model with the graph store that is used to compute embeddings.
     */
    static void validateTrainConfig(
        GraphStore graphStore,
        GraphSageBaseConfig config
    ) {
        var trainConfig = ModelCatalog
            .get(config.username(), config.modelName(), ModelData.class, GraphSageTrainConfig.class)
            .trainConfig();
        GraphStoreValidation.validate(
            graphStore,
            trainConfig
        );
    }

}
