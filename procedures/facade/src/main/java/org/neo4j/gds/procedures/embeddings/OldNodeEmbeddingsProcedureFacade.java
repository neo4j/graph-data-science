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
package org.neo4j.gds.procedures.embeddings;

import org.neo4j.gds.algorithms.embeddings.NodeEmbeddingsAlgorithmStreamBusinessFacade;
import org.neo4j.gds.algorithms.embeddings.NodeEmbeddingsAlgorithmsEstimateBusinessFacade;
import org.neo4j.gds.algorithms.embeddings.NodeEmbeddingsAlgorithmsMutateBusinessFacade;
import org.neo4j.gds.algorithms.embeddings.NodeEmbeddingsAlgorithmsTrainBusinessFacade;
import org.neo4j.gds.algorithms.embeddings.NodeEmbeddingsAlgorithmsWriteBusinessFacade;
import org.neo4j.gds.procedures.algorithms.configuration.ConfigurationCreator;
import org.neo4j.gds.procedures.embeddings.graphsage.GraphSageProcedure;
import org.neo4j.gds.procedures.embeddings.hashgnn.HashGNNProcedure;
import org.neo4j.gds.procedures.embeddings.node2vec.Node2VecProcedure;

public class OldNodeEmbeddingsProcedureFacade {

    private final HashGNNProcedure hashGNN;
    private final Node2VecProcedure node2Vec;
    private final GraphSageProcedure graphSage;
    // business logic

    public OldNodeEmbeddingsProcedureFacade(
        ConfigurationCreator configurationCreator,
        NodeEmbeddingsAlgorithmsEstimateBusinessFacade estimateBusinessFacade,
        NodeEmbeddingsAlgorithmsMutateBusinessFacade mutateBusinessFacade,
        NodeEmbeddingsAlgorithmStreamBusinessFacade streamBusinessFacade,
        NodeEmbeddingsAlgorithmsTrainBusinessFacade trainBusinessFacade,
        NodeEmbeddingsAlgorithmsWriteBusinessFacade writeBusinessFacade
    ) {
        this.hashGNN = new HashGNNProcedure(
            configurationCreator,
            estimateBusinessFacade,
            mutateBusinessFacade,
            streamBusinessFacade
        );

        this.node2Vec = new Node2VecProcedure(
            configurationCreator,
            estimateBusinessFacade,
            mutateBusinessFacade,
            streamBusinessFacade,
            writeBusinessFacade
        );

        this.graphSage = new GraphSageProcedure(
            configurationCreator,
            estimateBusinessFacade,
            mutateBusinessFacade,
            streamBusinessFacade,
            trainBusinessFacade,
            writeBusinessFacade
        );
    }

    public HashGNNProcedure hashGNN() {
        return hashGNN;
    }

    public Node2VecProcedure node2Vec() {
        return node2Vec;
    }

    public GraphSageProcedure graphSage() {
        return graphSage;
    }


}
