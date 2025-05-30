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
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel;
import org.neo4j.gds.applications.algorithms.machinery.WriteRelationshipService;
import org.neo4j.gds.applications.algorithms.machinery.WriteStep;
import org.neo4j.gds.applications.algorithms.metadata.RelationshipsWritten;
import org.neo4j.gds.core.utils.progress.JobId;
import org.neo4j.gds.spanningtree.SpanningGraph;
import org.neo4j.gds.spanningtree.SpanningTree;
import org.neo4j.gds.spanningtree.SpanningTreeWriteConfig;

class SpanningTreeWriteStep implements WriteStep<SpanningTree, RelationshipsWritten> {
    private final WriteRelationshipService writeRelationshipService;
    private final SpanningTreeWriteConfig configuration;

    SpanningTreeWriteStep(
        WriteRelationshipService writeRelationshipService,
        SpanningTreeWriteConfig configuration
    ) {
        this.writeRelationshipService = writeRelationshipService;
        this.configuration = configuration;
    }

    @Override
    public RelationshipsWritten execute(
        Graph graph,
        GraphStore graphStore,
        ResultStore resultStore,
        SpanningTree result,
        JobId jobId
    ) {
        var spanningGraph = new SpanningGraph(graph, result);


        return writeRelationshipService.writeFromGraph(
            configuration.writeRelationshipType(),
            configuration.writeProperty(),
            spanningGraph,
            spanningGraph.rootIdMap(),
            AlgorithmLabel.SpanningTree.asString(),
            configuration.resolveResultStore(resultStore),
            (a,b,c)-> true,
            configuration.jobId()
        );
    }
}
