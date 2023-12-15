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
package org.neo4j.gds.algorithms.embeddings;

import org.neo4j.gds.algorithms.AlgorithmComputationResult;
import org.neo4j.gds.algorithms.StreamComputationResult;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageResult;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageStreamConfig;
import org.neo4j.gds.embeddings.node2vec.Node2VecResult;
import org.neo4j.gds.embeddings.node2vec.Node2VecStreamConfig;

public class NodeEmbeddingsAlgorithmStreamBusinessFacade {
    private final NodeEmbeddingsAlgorithmsFacade nodeEmbeddingsAlgorithmsFacade;

    public NodeEmbeddingsAlgorithmStreamBusinessFacade(NodeEmbeddingsAlgorithmsFacade communityAlgorithmsFacade) {
        this.nodeEmbeddingsAlgorithmsFacade = communityAlgorithmsFacade;
    }

    public StreamComputationResult<Node2VecResult> node2Vec(
        String graphName,
        Node2VecStreamConfig config
    ) {
        var result = this.nodeEmbeddingsAlgorithmsFacade.node2Vec(
            graphName,
            config
        );

        return createStreamComputationResult(result);
    }

    public StreamComputationResult<GraphSageResult> graphSage(
        String graphName,
        GraphSageStreamConfig config
    ) {
        var result = this.nodeEmbeddingsAlgorithmsFacade.graphSage(
            graphName,
            config
        );

        return createStreamComputationResult(result);
    }

    private <RESULT> StreamComputationResult<RESULT> createStreamComputationResult(AlgorithmComputationResult<RESULT> result) {

        return StreamComputationResult.of(
            result.result(),
            result.graph()
        );

    }

}
