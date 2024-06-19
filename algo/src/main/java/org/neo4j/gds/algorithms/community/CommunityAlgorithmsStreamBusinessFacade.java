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
package org.neo4j.gds.algorithms.community;

import org.neo4j.gds.algorithms.AlgorithmComputationResult;
import org.neo4j.gds.algorithms.StreamComputationResult;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.scc.SccBaseConfig;
import org.neo4j.gds.triangle.LocalClusteringCoefficientResult;
import org.neo4j.gds.triangle.LocalClusteringCoefficientStreamConfig;
import org.neo4j.gds.triangle.TriangleCountBaseConfig;
import org.neo4j.gds.triangle.TriangleCountResult;

public class CommunityAlgorithmsStreamBusinessFacade {
    private final CommunityAlgorithmsFacade communityAlgorithmsFacade;

    public CommunityAlgorithmsStreamBusinessFacade(CommunityAlgorithmsFacade communityAlgorithmsFacade) {
        this.communityAlgorithmsFacade = communityAlgorithmsFacade;
    }

    public StreamComputationResult<HugeLongArray> scc(
        String graphName,
        SccBaseConfig config
    ) {

        var result = this.communityAlgorithmsFacade.scc(
            graphName,
            config
        );

        return createStreamComputationResult(result);
    }

    public StreamComputationResult<TriangleCountResult> triangleCount(
        String graphName,
        TriangleCountBaseConfig config
    ) {

        var result = this.communityAlgorithmsFacade.triangleCount(
            graphName,
            config
        );

        return createStreamComputationResult(result);
    }

    public StreamComputationResult<LocalClusteringCoefficientResult> localClusteringCoefficient(
        String graphName,
        LocalClusteringCoefficientStreamConfig config
    ) {
        var result = this.communityAlgorithmsFacade.localClusteringCoefficient(
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
