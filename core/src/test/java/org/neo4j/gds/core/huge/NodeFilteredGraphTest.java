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

import org.apache.commons.lang3.mutable.MutableLong;
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
import org.neo4j.gds.gdl.GdlFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

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

        NodeFilteredGraph filteredGraph = (NodeFilteredGraph) graphStore.getGraph(
            filterLabel,
            RelationshipType.ALL_RELATIONSHIPS,
            Optional.empty()
        );

        assertEquals(4L, filteredGraph.nodeCount());


        unfilteredGraph.forEachNode(nodeId -> {
            long filteredNodeId = filteredGraph.toFilteredNodeId(nodeId);
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

        var expected = Map.<Long, List<String>>of(
            filteredIdFunction.apply("a"), List.of("b"),
            filteredIdFunction.apply("b"), List.of("c", "d"),
            filteredIdFunction.apply("c"), List.of()
        );

        expected.forEach((source, targets) -> {
            assertThat(graph.streamRelationships(source, Double.NaN)
                .mapToLong(RelationshipCursor::targetId)).containsExactlyInAnyOrder(targets.stream()
                .map(filteredIdFunction)
                .toArray(Long[]::new));
        });
    }

    @Test
    void containsOnFilteredGraph() {
        var personGraph = graphStore.getGraph(NodeLabel.of("Person"));
        assertThat(personGraph.containsOriginalId(idFunction.of("a"))).isTrue();
        assertThat(personGraph.containsOriginalId(idFunction.of("b"))).isTrue();
        assertThat(personGraph.containsOriginalId(idFunction.of("c"))).isTrue();
        assertThat(personGraph.containsOriginalId(idFunction.of("d"))).isTrue();

        assertThat(personGraph.containsOriginalId(idFunction.of("x"))).isFalse();
        assertThat(personGraph.containsOriginalId(idFunction.of("e"))).isFalse();
    }

    @Test
    void shouldReturnLabelSpecificNodeCount() {
        var personLabel = NodeLabel.of("Person");
        var personGraph = graphStore.getGraph(personLabel);

        assertThat(personGraph.nodeCount()).isEqualTo(personGraph.nodeCount(personLabel));
        assertThat(personGraph.nodeCount(personLabel)).isEqualTo(4);
    }

    @Test
    void shouldStreamRelationshipsCorrectly() {
        var ids = new MutableLong(42);
        String gdl = " (a:A)," +
                     " (b:B)," +
                     " (a2:A)," +
                     " (a)-[:REL]->(b)," +
                     " (a)-[:REL]->(a2)," +
                     " (a2)-[:REL]->(a)" +
                     " (a2)-[:REL]->(b)" +
                     " (a2)-[:REL]->(a2)";

        var graphStore = GdlFactory.builder()
            .graphName("test")
            .gdlGraph(gdl)
            .nodeIdFunction(ids::getAndIncrement)
            .build()
            .build();

        var nodeFilteredGraph = graphStore.getGraph(NodeLabel.of("A"), RelationshipType.of("REL"), Optional.empty());

        nodeFilteredGraph.forEachNode(id -> {
            var degree = nodeFilteredGraph.degree(id);

            MutableLong degree_from_stream_relationship = new MutableLong();
            nodeFilteredGraph.streamRelationships(id, 0).forEach(rel -> {
                degree_from_stream_relationship.increment();
            });
            assertThat(degree_from_stream_relationship.longValue()).isEqualTo(degree);

            return true;
        });

    }

    Function<String, Long> filteredIdFunction(Graph graph) {
        return (variable) -> graph.toMappedNodeId(idFunction.of(variable));
    }
}
