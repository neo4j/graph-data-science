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
package org.neo4j.gds.pathfinding;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.ResultStore;
import org.neo4j.gds.applications.algorithms.machinery.WriteRelationshipService;
import org.neo4j.gds.applications.algorithms.machinery.WriteStep;
import org.neo4j.gds.applications.algorithms.metadata.RelationshipsWritten;
import org.neo4j.gds.core.JobId;
import org.neo4j.gds.paths.bellmanford.BellmanFordResult;

import java.util.Optional;
import java.util.function.Function;

public final class BellmanFordWriteStep implements WriteStep<BellmanFordResult, RelationshipsWritten> {
    private final WriteRelationshipService writeRelationshipService;
    private final String writeRelationshipType;
    private final boolean writeNegativeCycles;
    private final boolean writeNodeIds;
    private final boolean writeCosts;
    private final Function<ResultStore, Optional<ResultStore>> resultStoreResolver;
    private final JobId jobId;

    public BellmanFordWriteStep(
        WriteRelationshipService writeRelationshipService,
        String writeRelationshipType,
        boolean writeNegativeCycles,
        boolean writeNodeIds,
        boolean writeCosts,
        Function<ResultStore, Optional<ResultStore>> resultStoreResolver,
        JobId jobId
    ) {
        this.writeRelationshipService = writeRelationshipService;
        this.writeRelationshipType = writeRelationshipType;
        this.writeNegativeCycles = writeNegativeCycles;
        this.writeNodeIds = writeNodeIds;
        this.writeCosts = writeCosts;
        this.resultStoreResolver = resultStoreResolver;
        this.jobId = jobId;
    }

    @Override
    public RelationshipsWritten execute(
        Graph graph,
        GraphStore graphStore,
        ResultStore resultStore,
        BellmanFordResult result,
        JobId jobId
    ) {
        var specification = new PathFindingWriteRelationshipSpecification(graph, writeNodeIds, writeCosts);
        var keys = specification.createKeys();
        var types = specification.createTypes();

        var paths = result.shortestPaths();
        if (writeNegativeCycles && result.containsNegativeCycle()) {
            paths = result.negativeCycles();
        }
        try (
            var relationshipStream = paths.mapPaths(specification::createRelationship);
        ) {

            return writeRelationshipService.writeFromRelationshipStream(
                writeRelationshipType,
                keys,
                types,
                relationshipStream,
                graph,
                "Write shortest Paths",
                resultStoreResolver.apply(resultStore),
                this.jobId
            );
        }
    }

}
