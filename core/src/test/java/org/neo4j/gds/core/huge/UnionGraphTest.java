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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.RelationshipCursor;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;
import org.neo4j.gds.gdl.GdlFactory;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertFalse;

@GdlExtension
class UnionGraphTest {

    @GdlGraph(graphNamePrefix = "natural")
    @GdlGraph(graphNamePrefix = "undirected", orientation = Orientation.UNDIRECTED)
    private static final String GDL = "()-->()-->()";
    @Inject
    TestGraph naturalGraph;
    @Inject
    TestGraph undirectedGraph;

    @GdlGraph(graphNamePrefix = "directedA")
    private static final String labeled_GDL = "()-[:A]->()";
    @Inject
    TestGraph directedAGraph;

    @Test
    void conflictingDirectionThrowsException() {
        Graph unionGraph = UnionGraph.of(List.of(naturalGraph, undirectedGraph));

        assertThatThrownBy(unionGraph::schema)
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Conflicting directionality for relationship types __ALL__");
    }

    @Test
    void canUnionDirectedAndUndirectedGraph() {
        Graph unionGraph1 = UnionGraph.of(List.of(undirectedGraph, directedAGraph));
        Graph unionGraph2 = UnionGraph.of(List.of(directedAGraph, undirectedGraph));

        assertFalse(unionGraph1.schema().isUndirected());
        assertFalse(unionGraph2.schema().isUndirected());
    }


    @GdlGraph(graphNamePrefix = "multiRelType")
    private static final String MULTI_REL_TYPE_GDL =
        "CREATE" +
        "  (a:Node1)" +
        ", (b:Node1)" +
        ", (c:Node1)" +
        ", (a)-[:REL1]->(b)" +
        ", (a)-[:REL2]->(c)" +

        ", (b)-[:REL1]->(a)" +
        ", (b)-[:REL1]->(c)" +

        ", (c)-[:REL2]->(a)" +
        ", (c)-[:REL2]->(b)";

    @Inject
    GraphStore multiRelTypeGraphStore;

    @Inject
    IdFunction multiRelTypeIdFunction;

    @ParameterizedTest
    @MethodSource("nodeRelCombinations")
    void shouldSelectGivenRelationships(String sourceVariable, Set<RelationshipType> relTypes, Collection<String> targetVariables) {
        IdFunction mappedIdFunction = name -> multiRelTypeGraphStore.nodes().toMappedNodeId(multiRelTypeIdFunction.of(name));

        Graph graph = multiRelTypeGraphStore.getUnion();

        var filteredGraph = graph.relationshipTypeFilteredGraph(relTypes);
        long[] actualTargets = filteredGraph
            .streamRelationships(
                mappedIdFunction.of(sourceVariable),
                Double.NaN
            )
            .mapToLong(RelationshipCursor::targetId).toArray();
        long[] expectedTargets = targetVariables.stream().mapToLong(mappedIdFunction::of).toArray();

        assertThat(actualTargets).containsExactlyInAnyOrder(expectedTargets);
    }

    static Stream<Arguments> nodeRelCombinations() {
        return Stream.of(
            Arguments.of("a", Set.of(RelationshipType.of("REL1")), List.of("b")),
            Arguments.of("a", Set.of(RelationshipType.of("REL2")), List.of("c")),
            Arguments.of("b", Set.of(RelationshipType.of("REL1")), List.of("a", "c")),
            Arguments.of("c", Set.of(RelationshipType.of("REL2")), List.of("a", "b")),
            Arguments.of("a", Set.of(), List.of("b", "c"))
        );
    }

    private static Stream<Arguments> relationshipPropertyGraphs() {
        return Stream.of(
            Arguments.of(
                "  (a)-[:FIRST { times: 5 }]->(b)" +
                ", (a)-[:SECOND]->(b)",
                "first defines property"
            ),
            Arguments.of(
                "  (a)-[:FIRST]->(b)" +
                ", (a)-[:SECOND { times: 5 }]->(b)",
                "second defines property"
            )
        );
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("relationshipPropertyGraphs")
    void relationshipProperty(String gdlGraph, String desc) {
        var graphFactory = GdlFactory.of(gdlGraph);
        var graph = graphFactory.build().getUnion();
        var a = graphFactory.nodeId("a");
        var b = graphFactory.nodeId("b");

        assertThat(graph.relationshipProperty(a, b)).isEqualTo(5D);
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("relationshipPropertyGraphs")
    void relationshipPropertyWithFallBack(String gdlGraph, String desc) {
        var graphFactory = GdlFactory.of(gdlGraph);
        var graph = graphFactory.build().getUnion();
        var a = graphFactory.nodeId("a");
        var b = graphFactory.nodeId("b");

        assertThat(graph.relationshipProperty(a, b, -1)).isEqualTo(5D);
    }

    @Test
    void overlappingRelationshipProperty() {
        var graphFactory = GdlFactory.of(
            "  (a)-[:FIRST { times: 3 }]->(b)" +
            ", (a)-[:SECOND { times: 5 }]->(b)");
        var graph = graphFactory.build().getUnion();
        var a = graphFactory.nodeId("a");
        var b = graphFactory.nodeId("b");

        assertThat(graph.relationshipProperty(a, b)).isIn(3D, 5D);
        assertThat(graph.relationshipProperty(a, b, -1)).isIn(3D, 5D);
    }

    @Test
    void filteredGraphRootNodeCountShouldAgreeWithUnfilteredGraph() {
        // A graph that is both label filtered and has multiple relationship types
        var graphStore = GdlFactory.of(
            "CREATE " +
            "  (a:A)" +
            ", (b:B)" +
            ", (a)-[:R1]->(b)" +
            ", (b)-[:R2]->(a)"
        ).build();

        var unionGraph = graphStore.getGraph(
            List.of(NodeLabel.of("A")),
            List.of(RelationshipType.of("R1"), RelationshipType.of("R2")),
            Optional.empty()
        );

        assertThat(unionGraph.rootNodeCount().getAsLong()).isEqualTo(graphStore.nodeCount());
    }

    @Test
    void shouldAddNodeIdToNodeLabel() {
        var graphStore = GdlFactory.of(
            "CREATE " +
            "  (a:A)" +
            ", (b:B)" +
            ", (a)-[:R1]->(b)" +
            ", (b)-[:R2]->(a)"
        ).build();

        var unionGraph = graphStore.getGraph(
            List.of(NodeLabel.of("B")),
            List.of(RelationshipType.of("R1"), RelationshipType.of("R2")),
            Optional.empty()
        );

        unionGraph.addNodeLabel(NodeLabel.of("Test"));
        unionGraph.addNodeIdToLabel(0, NodeLabel.of("Test"));
        assertThat(unionGraph.nodeLabels(0))
            .containsExactlyInAnyOrder(
                NodeLabel.of("B"),
                NodeLabel.of("Test")
            );
    }
}
