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
package org.neo4j.gds.algorithms.similarity;

import org.neo4j.gds.algorithms.AlgorithmComputationResult;
import org.neo4j.gds.algorithms.StreamComputationResult;
import org.neo4j.gds.similarity.filterednodesim.FilteredNodeSimilarityStreamConfig;
import org.neo4j.gds.similarity.nodesim.NodeSimilarityResult;

public class SimilarityAlgorithmsStreamBusinessFacade {

    private final SimilarityAlgorithmsFacade similarityAlgorithmsFacade;

    public SimilarityAlgorithmsStreamBusinessFacade(SimilarityAlgorithmsFacade similarityAlgorithmsFacade) {
        this.similarityAlgorithmsFacade = similarityAlgorithmsFacade;
    }

    public StreamComputationResult<NodeSimilarityResult> filteredNodeSimilarity(
        String graphName,
        FilteredNodeSimilarityStreamConfig config
    ) {
        var result = similarityAlgorithmsFacade.filteredNodeSimilarity(graphName, config);

        return createStreamComputationResult(result);
    }

    // FIXME: the following method is duplicate, find a good place for it.
    private <RESULT> StreamComputationResult<RESULT> createStreamComputationResult(AlgorithmComputationResult<RESULT> result) {
        return StreamComputationResult.of(
            result.result(),
            result.graph()
        );
    }
    //FIXME: here ends the fixme-block

}
