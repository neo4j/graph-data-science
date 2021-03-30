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
package org.neo4j.graphalgo.core.utils.export.file;


import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.RelationshipType;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphStore;
import org.neo4j.graphalgo.core.loading.construction.GraphFactory;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.extension.GdlExtension;
import org.neo4j.graphalgo.extension.GdlGraph;
import org.neo4j.graphalgo.extension.Inject;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

@GdlExtension
class GraphStoreRelationshipVisitorTest {

    @GdlGraph
    static String DB_CYPHER = "CREATE (a:A)-[:R]->(b:A)-[:R1]->(c:B)-[:R1]->(a)";

    @Inject
    GraphStore graphStore;

    @Inject
    Graph graph;

    @Test
    void shouldAddRelationshipsToRelationshipBuilder() {
        var relationshipSchema = graphStore.schema().relationshipSchema();
        var relationshipsBuilderBuilder = GraphFactory.initRelationshipsBuilder()
            .concurrency(1)
            .nodes(graph)
            .tracker(AllocationTracker.empty());

        var relationshipVisitor = new GraphStoreRelationshipVisitor(relationshipSchema, relationshipsBuilderBuilder);

        var relationshipTypeR = RelationshipType.of("R");
        var relationshipTypeR1 = RelationshipType.of("R1");
        graph.forEachNode(nodeId -> {
            visitRelationshipType(relationshipVisitor, nodeId, relationshipTypeR);
            visitRelationshipType(relationshipVisitor, nodeId, relationshipTypeR1);
            return true;
        });

        var actualRelationships = relationshipVisitor.result();
        assertThat(actualRelationships.relationshipCount()).isEqualTo(3L);

        assertThat(actualRelationships.relationshipTypesWithTopology().get(relationshipTypeR).elementCount()).isEqualTo(1L);
        assertThat(actualRelationships.relationshipTypesWithTopology().get(relationshipTypeR1).elementCount()).isEqualTo(2L);
    }

    private void visitRelationshipType(GraphStoreRelationshipVisitor relationshipVisitor, long nodeId, RelationshipType relationshipType) {
        graph
            .relationshipTypeFilteredGraph(Set.of(relationshipType))
            .forEachRelationship(nodeId, (source, target) -> {
                relationshipVisitor.startId(source);
                relationshipVisitor.endId(target);
                relationshipVisitor.type(relationshipType.name());
                relationshipVisitor.endOfEntity();
                return true;
            });
    }
}
