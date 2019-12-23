/*
 * Copyright (c) 2017-2019 "Neo4j,"
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

import org.neo4j.graphalgo.AlgorithmFactory;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.impl.pagerank.LabsPageRankAlgorithmType;
import org.neo4j.graphalgo.impl.pagerank.PageRank;
import org.neo4j.graphdb.Node;
import org.neo4j.logging.Log;

import java.util.List;
import java.util.stream.LongStream;

class EigenvectorCentralityAlgorithmFactory extends AlgorithmFactory<PageRank, EigenvectorCentralityConfig> {
    @Override
    public PageRank build(
        Graph graph,
        EigenvectorCentralityConfig configuration,
        AllocationTracker tracker,
        Log log
    ) {
        PageRank.Config algoConfig = new PageRank.Config(
            configuration.maxIterations(),
            1.0,
            PageRank.DEFAULT_TOLERANCE
        );
        List<Node> sourceNodes = configuration.sourceNodes();
        LongStream sourceNodeIds = sourceNodes.stream().mapToLong(Node::getId);
        return LabsPageRankAlgorithmType.EIGENVECTOR_CENTRALITY
            .create(
                graph,
                Pools.DEFAULT,
                configuration.concurrency(),
                ParallelUtil.DEFAULT_BATCH_SIZE,
                algoConfig,
                sourceNodeIds,
                tracker
            );
    }

    @Override
    public MemoryEstimation memoryEstimation(EigenvectorCentralityConfig configuration) {
        throw new UnsupportedOperationException("Estimation is not implemented for this algorithm.");
    }
}
