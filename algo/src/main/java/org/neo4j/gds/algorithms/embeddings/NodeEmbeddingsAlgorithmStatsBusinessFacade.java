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

import org.neo4j.gds.algorithms.StatsResult;
import org.neo4j.gds.algorithms.runner.AlgorithmRunner;
import org.neo4j.gds.embeddings.fastrp.FastRPStatsConfig;


public class NodeEmbeddingsAlgorithmStatsBusinessFacade {
    private final NodeEmbeddingsAlgorithmsFacade nodeEmbeddingsAlgorithmsFacade;

    public NodeEmbeddingsAlgorithmStatsBusinessFacade(NodeEmbeddingsAlgorithmsFacade communityAlgorithmsFacade) {
        this.nodeEmbeddingsAlgorithmsFacade = communityAlgorithmsFacade;
    }

    public StatsResult<Long> fastRP(
        String graphName,
        FastRPStatsConfig configuration
    ) {
        // 1. Run the algorithm and time the execution
        var intermediateResult = AlgorithmRunner.runWithTiming(
            () -> nodeEmbeddingsAlgorithmsFacade.fastRP(graphName, configuration)
        );

        var algorithmResult = intermediateResult.algorithmResult;

        var statsResultBuilder = StatsResult.<Long>builder()
            .computeMillis(intermediateResult.computeMilliseconds)
            .postProcessingMillis(0L);

        algorithmResult.result().ifPresentOrElse(
            result -> {
                var nodeCount = algorithmResult.graph().nodeCount();
                statsResultBuilder.algorithmSpecificFields(nodeCount);
            },
            () -> statsResultBuilder.algorithmSpecificFields(0L)
        );

        return statsResultBuilder.build();
    }
}
