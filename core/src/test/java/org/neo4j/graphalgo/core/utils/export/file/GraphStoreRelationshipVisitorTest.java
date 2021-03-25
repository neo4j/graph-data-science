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
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphStore;
import org.neo4j.graphalgo.core.loading.construction.GraphFactory;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.extension.GdlExtension;
import org.neo4j.graphalgo.extension.GdlGraph;
import org.neo4j.graphalgo.extension.Inject;

import static org.assertj.core.api.Assertions.assertThat;

@GdlExtension
class GraphStoreRelationshipVisitorTest {

    @GdlGraph
    static String DB_CYPHER = "CREATE (a:A)-[:R]->(b:A)-[:R]->(c:B)";

    @Inject
    GraphStore graphStore;

    @Inject
    Graph graph;

    @Test
    void shouldAddRelationshipsToRelationshipBuilder() {
        var relationshipSchema = graphStore.schema().relationshipSchema();
        var relationshipsBuilder = GraphFactory.initRelationshipsBuilder()
            .concurrency(1)
            .nodes(graph)
            .tracker(AllocationTracker.empty())
            .build();

        var relationshipVisitor = new GraphStoreRelationshipVisitor(relationshipSchema, relationshipsBuilder);

        graph.forEachNode(nodeId -> {
            graph.forEachRelationship(nodeId, (source, target) -> {
                relationshipVisitor.startId(source);
                relationshipVisitor.endId(target);
                relationshipVisitor.type("R"); // TODO: set this dynamically?
                relationshipVisitor.endOfEntity();
                return true;
            });

            return  true;
        });

        var actualRelationships = relationshipsBuilder.build();
        var actualTopology = actualRelationships.topology();
        assertThat(actualTopology.elementCount()).isEqualTo(2L);
        assertThat(actualRelationships.properties()).isEmpty();
    }
}
