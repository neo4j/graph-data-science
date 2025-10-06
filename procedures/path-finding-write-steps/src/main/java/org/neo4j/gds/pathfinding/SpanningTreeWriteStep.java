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
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel;
import org.neo4j.gds.applications.algorithms.machinery.WriteRelationshipService;
import org.neo4j.gds.applications.algorithms.machinery.WriteStep;
import org.neo4j.gds.applications.algorithms.metadata.RelationshipsWritten;
import org.neo4j.gds.core.JobId;
import org.neo4j.gds.spanningtree.SpanningGraph;
import org.neo4j.gds.spanningtree.SpanningTree;

import java.util.Optional;
import java.util.function.Function;

public class SpanningTreeWriteStep implements WriteStep<SpanningTree, RelationshipsWritten> {
    private final WriteRelationshipService writeRelationshipService;
    private final String writeRelationshipType;
    private final String writeProperty;
    private final Function<ResultStore, Optional<ResultStore>> resultStoreResolver;
    private final JobId jobId;

    public SpanningTreeWriteStep(
        WriteRelationshipService writeRelationshipService,
        String writeRelationshipType,
        String writeProperty,
        Function<ResultStore, Optional<ResultStore>> resultStoreResolver,
        JobId jobId
    ) {
        this.writeRelationshipService = writeRelationshipService;
        this.writeRelationshipType = writeRelationshipType;
        this.writeProperty = writeProperty;
        this.resultStoreResolver = resultStoreResolver;
        this.jobId = jobId;
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
            writeRelationshipType,
            writeProperty,
            spanningGraph,
            graph,
            AlgorithmLabel.SpanningTree.asString(),
            resultStoreResolver.apply(resultStore),
            (a,b,c)-> true,
            this.jobId
        );
    }
}
