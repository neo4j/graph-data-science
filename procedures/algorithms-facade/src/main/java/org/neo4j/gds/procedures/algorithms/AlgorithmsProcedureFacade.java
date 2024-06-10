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
package org.neo4j.gds.procedures.algorithms;

import org.neo4j.gds.procedures.algorithms.centrality.CentralityProcedureFacade;
import org.neo4j.gds.procedures.algorithms.community.CommunityProcedureFacade;
import org.neo4j.gds.procedures.algorithms.pathfinding.PathFindingProcedureFacade;
import org.neo4j.gds.procedures.algorithms.similarity.SimilarityProcedureFacade;

/**
 * This is the facade that faces pipelines, so everything you can pipeline I guess.
 * I assume you can't pipeline a pipeline...
 */
public class AlgorithmsProcedureFacade {
    private final CentralityProcedureFacade centralityProcedureFacade;
    private final CommunityProcedureFacade communityProcedureFacade;
    private final PathFindingProcedureFacade pathFindingProcedureFacade;
    private final SimilarityProcedureFacade similarityProcedureFacade;

    public AlgorithmsProcedureFacade(
        CentralityProcedureFacade centralityProcedureFacade,
        CommunityProcedureFacade communityProcedureFacade,
        PathFindingProcedureFacade pathFindingProcedureFacade,
        SimilarityProcedureFacade similarityProcedureFacade
    ) {
        this.centralityProcedureFacade = centralityProcedureFacade;
        this.communityProcedureFacade = communityProcedureFacade;
        this.pathFindingProcedureFacade = pathFindingProcedureFacade;
        this.similarityProcedureFacade = similarityProcedureFacade;
    }

    public CentralityProcedureFacade centrality() {
        return centralityProcedureFacade;
    }

    public CommunityProcedureFacade community() {
        return communityProcedureFacade;
    }

    public PathFindingProcedureFacade pathFinding() {
        return pathFindingProcedureFacade;
    }

    public SimilarityProcedureFacade similarity() {
        return similarityProcedureFacade;
    }
}
