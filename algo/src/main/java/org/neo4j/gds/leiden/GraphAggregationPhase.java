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
package org.neo4j.gds.leiden;

import org.neo4j.gds.Orientation;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.loading.construction.GraphFactory;
import org.neo4j.gds.core.loading.construction.RelationshipsBuilder;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.partition.PartitionUtils;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import java.util.Optional;
import java.util.concurrent.ExecutorService;

class GraphAggregationPhase {

    private final Graph workingGraph;
    private final HugeLongArray communities;
    private final Orientation orientation;
    private final long maxCommunityId;
    private final ExecutorService executorService;
    private final int concurrency;
    private final TerminationFlag terminationFlag;
    private final ProgressTracker progressTracker;

    GraphAggregationPhase(
        Graph workingGraph,
        Orientation orientation,
        HugeLongArray communities,
        long maxCommunityId,
        ExecutorService executorService,
        int concurrency,
        TerminationFlag terminationFlag,
        ProgressTracker progressTracker
    ) {
        this.workingGraph = workingGraph;
        this.communities = communities;
        this.orientation = orientation;
        this.maxCommunityId = maxCommunityId;
        this.executorService = executorService;
        this.concurrency = concurrency;

        this.terminationFlag = terminationFlag;
        this.progressTracker = progressTracker;
    }

    Graph run() {
        var nodesBuilder = GraphFactory.initNodesBuilder()
            .maxOriginalId(maxCommunityId)
            .concurrency(this.concurrency)
            .build();

        terminationFlag.assertRunning();

        ParallelUtil.parallelForEachNode(
            workingGraph.nodeCount(),
            concurrency,
            (nodeId) -> {
                nodesBuilder.addNode(communities.get(nodeId));
            }
        );

        terminationFlag.assertRunning();

        IdMap idMap = nodesBuilder.build().idMap();
        RelationshipsBuilder relationshipsBuilder = GraphFactory.initRelationshipsBuilder()
            .nodes(idMap)
            .orientation(orientation)
            .addPropertyConfig(Aggregation.SUM, DefaultValue.forDouble())
            .executorService(executorService)
            .build();
        var relationshipCreators = PartitionUtils.degreePartition(
            workingGraph,
            concurrency,
            partition ->
                new RelationshipCreator(
                    communities, partition, relationshipsBuilder, workingGraph.concurrentCopy(), orientation, progressTracker
                ),
            Optional.empty()
        );

        ParallelUtil.run(relationshipCreators, executorService);

        return GraphFactory.create(idMap, relationshipsBuilder.build());
    }

}
