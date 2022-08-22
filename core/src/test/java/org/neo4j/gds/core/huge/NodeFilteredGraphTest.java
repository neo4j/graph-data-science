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
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.api.RelationshipCursor;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;

import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

@GdlExtension
class NodeFilteredGraphTest {

    @GdlGraph(idOffset = 1337)
    static String GDL = " (x:Ignore)," +
                        " (a:Person)," +
                        " (b:Ignore:Person)," +
                        " (c:Ignore:Person)," +
                        " (d:Person)," +
                        " (e:Ignore)," +
                        " (a)-->(b)," +
                        " (a)-->(e)," +
                        " (b)-->(c)," +
                        " (b)-->(d)," +
                        " (c)-->(e)";

    @Inject
    GraphStore graphStore;

    @Inject
    IdFunction idFunction;

    @Test
    void filteredIdMapThatIncludesAllNodes() {
        Graph unfilteredGraph = graphStore.getGraph(RelationshipType.ALL_RELATIONSHIPS);

        NodeLabel filterLabel = NodeLabel.of("Person");

        Graph filteredGraph = graphStore.getGraph(
            filterLabel,
            RelationshipType.ALL_RELATIONSHIPS,
            Optional.empty()
        );

        assertEquals(4L, filteredGraph.nodeCount());


        unfilteredGraph.forEachNode(nodeId -> {
            long filteredNodeId = filteredGraph.fromRootNodeId(nodeId);
            if (unfilteredGraph.hasLabel(nodeId, filterLabel)) {
                assertThat(filteredGraph.toOriginalNodeId(filteredNodeId)).isEqualTo(unfilteredGraph.toOriginalNodeId(nodeId));
            } else {
                assertThat(filteredNodeId).isEqualTo(IdMap.NOT_FOUND);
            }

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
        assertThat(graph.relationshipCount()).isEqualTo(3L);
    }

    @Test
    void filterDegree() {
        var graph = graphStore.getGraph(
            NodeLabel.of("Person"),
            RelationshipType.ALL_RELATIONSHIPS,
            Optional.empty()
        );

        assertThat(graph.degree(filteredIdFunction(graph).apply("a"))).isEqualTo(1L);
    }

    @Test
    void filterDegreeWithoutParallelRelationships() {
        var graph = graphStore.getGraph(
            NodeLabel.of("Person"),
            RelationshipType.ALL_RELATIONSHIPS,
            Optional.empty()
        );

        assertThat(graph.degreeWithoutParallelRelationships(filteredIdFunction(graph).apply("a"))).isEqualTo(1L);
    }

    @Test
    void filterStreamRelationships() {
        var graph = graphStore.getGraph(
            NodeLabel.of("Person"),
            RelationshipType.ALL_RELATIONSHIPS,
            Optional.empty()
        );

        Function<String, Long> filteredIdFunction = (variable) -> graph.toMappedNodeId(idFunction.of(variable));

        assertThat(graph.streamRelationships(filteredIdFunction.apply("a"), Double.NaN).mapToLong(RelationshipCursor::targetId)).containsExactlyInAnyOrder(Stream.of("b").map(filteredIdFunction).toArray(Long[]::new));
        assertThat(graph.streamRelationships(filteredIdFunction.apply("b"), Double.NaN).mapToLong(RelationshipCursor::targetId)).containsExactlyInAnyOrder(Stream.of("c", "d").map(filteredIdFunction).toArray(Long[]::new));
        assertThat(graph.streamRelationships(filteredIdFunction.apply("c"), Double.NaN).mapToLong(RelationshipCursor::targetId)).isEmpty();
    }

    @Test
    void containsOnFilteredGraph() {
        var personGraph = graphStore.getGraph(NodeLabel.of("Person"));
        assertThat(personGraph.contains(idFunction.of("a"))).isTrue();
        assertThat(personGraph.contains(idFunction.of("b"))).isTrue();
        assertThat(personGraph.contains(idFunction.of("c"))).isTrue();
        assertThat(personGraph.contains(idFunction.of("d"))).isTrue();

        assertThat(personGraph.contains(idFunction.of("x"))).isFalse();
        assertThat(personGraph.contains(idFunction.of("e"))).isFalse();
    }

    Function<String, Long> filteredIdFunction(Graph graph) {
        return (variable) -> graph.toMappedNodeId(idFunction.of(variable));
    }
}
