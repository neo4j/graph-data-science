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
package org.neo4j.gds.core.huge;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

@GdlExtension
class NodeFilteredGraphTest {

    @GdlGraph
    static String GDL = " (a:Person)," +
                        " (b:Ignore:Person)," +
                        " (c:Ignore:Person)," +
                        " (d:Person)," +
                        " (e:Ignore)," +
                        " (a)-->(b)," +
                        " (a)-->(e)";

    @Inject
    GraphStore graphStore;

    @Inject
    IdFunction idFunction;

    @Test
    void filteredIdMapThatIncludesAllNodes() {
        Graph unfilteredGraph = graphStore.getGraph(RelationshipType.ALL_RELATIONSHIPS);

        Graph filteredGraph = graphStore.getGraph(
            NodeLabel.of("Person"),
            RelationshipType.ALL_RELATIONSHIPS,
            Optional.empty()
        );

        assertEquals(4L, filteredGraph.nodeCount());
        filteredGraph.forEachNode(nodeId -> {
            assertEquals(unfilteredGraph.toOriginalNodeId(nodeId), filteredGraph.toOriginalNodeId(nodeId));
            return true;
        });
    }

    @Test
    void shouldFilterRelationshipCount() {
        var graph = graphStore.getGraph(
            NodeLabel.of("Person"),
            RelationshipType.ALL_RELATIONSHIPS,
            Optional.empty()
        );
        assertThat(graph.relationshipCount()).isEqualTo(1L);
    }

    @Test
    void filterDegree() {
        var graph = graphStore.getGraph(
            NodeLabel.of("Person"),
            RelationshipType.ALL_RELATIONSHIPS,
            Optional.empty()
        );

        assertThat(graph.degree(idFunction.of("a"))).isEqualTo(1L);
    }

    @Test
    void filterDegreeWithoutParallelRelationships() {
        var graph = graphStore.getGraph(
            NodeLabel.of("Person"),
            RelationshipType.ALL_RELATIONSHIPS,
            Optional.empty()
        );

        assertThat(graph.degreeWithoutParallelRelationships(idFunction.of("a"))).isEqualTo(1L);
    }
}
