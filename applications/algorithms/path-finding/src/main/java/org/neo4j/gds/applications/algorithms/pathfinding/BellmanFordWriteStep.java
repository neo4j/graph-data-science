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
package org.neo4j.gds.applications.algorithms.pathfinding;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.ResultStore;
import org.neo4j.gds.applications.algorithms.machinery.WriteRelationshipService;
import org.neo4j.gds.applications.algorithms.machinery.WriteStep;
import org.neo4j.gds.applications.algorithms.metadata.RelationshipsWritten;
import org.neo4j.gds.core.utils.progress.JobId;
import org.neo4j.gds.paths.bellmanford.AllShortestPathsBellmanFordWriteConfig;
import org.neo4j.gds.paths.bellmanford.BellmanFordResult;

class BellmanFordWriteStep implements WriteStep<BellmanFordResult, RelationshipsWritten> {
    private final WriteRelationshipService writeRelationshipService;
    private final AllShortestPathsBellmanFordWriteConfig configuration;

    BellmanFordWriteStep(
        WriteRelationshipService writeRelationshipService,
        AllShortestPathsBellmanFordWriteConfig configuration
    ) {
        this.configuration = configuration;
        this.writeRelationshipService = writeRelationshipService;
    }

    @Override
    public RelationshipsWritten execute(
        Graph graph,
        GraphStore graphStore,
        ResultStore resultStore,
        BellmanFordResult result,
        JobId jobId
    ) {

        var writeNodeIds = configuration.writeNodeIds();
        var writeCosts = configuration.writeCosts();

        var specification = new PathFindingWriteRelationshipSpecification(graph,writeNodeIds,writeCosts);
        var keys = specification.createKeys();
        var types=  specification.createTypes();

        var paths = result.shortestPaths();
        if (configuration.writeNegativeCycles() && result.containsNegativeCycle()) {
            paths = result.negativeCycles();
        }
        try (
            var relationshipStream = paths.mapPaths(specification::createRelationship);
        ) {

            return writeRelationshipService.writeFromRelationshipStream(
                configuration.writeRelationshipType(),
                keys,
                types,
                relationshipStream,
                graph,
                "Write shortest Paths",
                configuration.resolveResultStore(resultStore),
                configuration.jobId()
            );
        }
    }

}
