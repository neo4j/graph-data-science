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
package org.neo4j.graphalgo.core.huge;

import org.apache.commons.lang3.tuple.Triple;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphStore;
import org.neo4j.graphalgo.extension.GdlExtension;
import org.neo4j.graphalgo.extension.GdlGraph;
import org.neo4j.graphalgo.extension.IdFunction;
import org.neo4j.graphalgo.extension.Inject;
import org.neo4j.graphalgo.extension.TestGraph;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
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

    @Test
    void isUndirectedOnlyIfAllInnerGraphsAre() {
        Graph unionGraph1 = UnionGraph.of(List.of(naturalGraph, undirectedGraph));
        Graph unionGraph2 = UnionGraph.of(List.of(undirectedGraph, naturalGraph));

        assertFalse(unionGraph1.isUndirected());
        assertFalse(unionGraph2.isUndirected());
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

    @Test
    void testGetTargetOnMultiRelationshipTypeGraph() {
        var combinations = List.of(
            Triple.of(multiRelTypeIdFunction.of("a"), 0, multiRelTypeIdFunction.of("b")),
            Triple.of(multiRelTypeIdFunction.of("a"), 1, multiRelTypeIdFunction.of("c")),
            Triple.of(multiRelTypeIdFunction.of("b"), 0, multiRelTypeIdFunction.of("a")),
            Triple.of(multiRelTypeIdFunction.of("b"), 1, multiRelTypeIdFunction.of("c")),
            Triple.of(multiRelTypeIdFunction.of("c"), 0, multiRelTypeIdFunction.of("a")),
            Triple.of(multiRelTypeIdFunction.of("c"), 1, multiRelTypeIdFunction.of("b"))
        );

        var graph = multiRelTypeGraphStore.getUnion();

        combinations.forEach(combination -> {
            var source = combination.getLeft();
            var index = combination.getMiddle();
            var expected = combination.getRight();

            assertEquals(expected, graph.getTarget(source, index));
        });
    }

//    @ParameterizedTest
//    @MethodSource("nodeRelCombinations")
//    void shouldSelectGivenRelationships(String sourceVariable, Set<RelationshipType> relTypes, Collection<String> targetVariables) {
//        Graph graph = multiRelTypeGraphStore.getUnion();
//
//        long[] actualTargets = graph
//            .streamRelationships(
//                multiRelTypeIdFunction.of(sourceVariable),
//                Double.NaN,
//                relTypes
//            )
//            .mapToLong(RelationshipCursor::targetId).toArray();
//        long[] expectedTargets = targetVariables.stream().mapToLong(multiRelTypeIdFunction::of).toArray();
//
//        assertThat(actualTargets).containsExactlyInAnyOrder(expectedTargets);
//    }
//
//    static Stream<Arguments> nodeRelCombinations() {
//        return Stream.of(
//            Arguments.of("a", Set.of(RelationshipType.of("REL1")), List.of("b")),
//            Arguments.of("a", Set.of(RelationshipType.of("REL2")), List.of("c")),
//            Arguments.of("b", Set.of(RelationshipType.of("REL1")), List.of("a", "c")),
//            Arguments.of("c", Set.of(RelationshipType.of("REL2")), List.of("a", "b")),
//            Arguments.of("a", Set.of(), List.of("b", "c"))
//        );
//    }
}
