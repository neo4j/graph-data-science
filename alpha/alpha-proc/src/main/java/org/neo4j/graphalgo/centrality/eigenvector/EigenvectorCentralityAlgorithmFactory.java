/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.centrality.eigenvector;

import org.neo4j.graphalgo.AlphaAlgorithmFactory;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.pagerank.LabsPageRankAlgorithmType;
import org.neo4j.graphalgo.pagerank.PageRank;
import org.neo4j.logging.Log;

class EigenvectorCentralityAlgorithmFactory extends AlphaAlgorithmFactory<PageRank, EigenvectorCentralityConfig> {
    @Override
    public PageRank build(
        Graph graph,
        EigenvectorCentralityConfig configuration,
        AllocationTracker tracker,
        Log log
    ) {
        configuration = ImmutableEigenvectorCentralityConfig.builder().from(configuration).dampingFactor(1.0).build();
        return LabsPageRankAlgorithmType.EIGENVECTOR_CENTRALITY
            .create(
                graph,
                configuration.sourceNodeIds(),
                configuration,
                configuration.concurrency(),
                Pools.DEFAULT,
                tracker
            );
    }
}
