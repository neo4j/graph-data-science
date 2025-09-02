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
import org.neo4j.gds.config.JobIdConfig;
import org.neo4j.gds.config.WriteRelationshipConfig;
import org.neo4j.gds.core.utils.progress.JobId;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.pathfinding.PathFindingWriteRelationshipSpecification;
import org.neo4j.gds.paths.WritePathOptionsConfig;
import org.neo4j.gds.paths.dijkstra.PathFindingResult;


/**
 * This is relationship writes as needed by path finding algorithms (for now).
 */
class ShortestPathWriteStep<CONFIGURATION extends WriteRelationshipConfig & WritePathOptionsConfig & JobIdConfig> implements
    WriteStep<PathFindingResult, RelationshipsWritten> {
    private final Log log;
    private final WriteRelationshipService writeRelationshipService;
    private final CONFIGURATION configuration;

    ShortestPathWriteStep(
        Log log,
        WriteRelationshipService writeRelationshipService,
        CONFIGURATION configuration
    ) {
        this.log = log;
        this.writeRelationshipService = writeRelationshipService;
        this.configuration = configuration;
    }

    /**
     * Here we translate and write relationships from path finding algorithms back to the database.
     * We do it synchronously, time it, and gather metadata about how many relationships we wrote.
     */
    @Override
    public RelationshipsWritten execute(
        Graph graph,
        GraphStore graphStore,
        ResultStore resultStore,
        PathFindingResult result,
        JobId jobId
    ) {
        var writeNodeIds = configuration.writeNodeIds();
        var writeCosts = configuration.writeCosts();

        var specification = new PathFindingWriteRelationshipSpecification(graph,writeNodeIds,writeCosts);
        var keys = specification.createKeys();
        var types=  specification.createTypes();

        /*
         * We have to ensure the stream closes, so that progress tracker closes.
         * It is abominable that we have to do this. To be fixed in the future, somehow.
         * The problem is that apparently progress tracker is keyed off of ths stream,
         * and that we cannot rely on whatever plugged in exporter comes along takes responsibility for these things.
         * Ergo we need this little block, but really we should engineer it all better.
         */
        try (
              var relationshipStream = result.mapPaths(specification::createRelationship);
        ) {

            // the final result is the side effect of writing to the database, plus this metadata
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
