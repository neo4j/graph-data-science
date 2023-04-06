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
package org.neo4j.gds.triangle;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.TestSupport;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.Pools;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.gds.Orientation.UNDIRECTED;
import static org.neo4j.gds.triangle.IntersectingTriangleCount.EXCLUDED_NODE_TRIANGLE_COUNT;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

class IntersectingTriangleCountTest {

    private static Stream<Arguments> noTriangleQueries() {
        return Stream.of(
            Arguments.of(fromGdl("CREATE ()-[:T]->()-[:T]->()"), "line"),
            Arguments.of(fromGdl("CREATE (), (), ()"), "no rels"),
            Arguments.of(fromGdl("CREATE ()-[:T]->(), ()"), "one rel"),
            Arguments.of(fromGdl("CREATE (a1)-[:T]->()-[:T]->(a1), ()"), "back and forth")
        );
    }

    @MethodSource("noTriangleQueries")
    @ParameterizedTest(name = "{1}")
    void noTriangles(Graph graph, String ignoredName) {
        TriangleCountResult result = compute(graph);

        assertEquals(0L, result.globalTriangles());
        assertEquals(3, result.localTriangles().size());
        assertEquals(0, result.localTriangles().get(0));
        assertEquals(0, result.localTriangles().get(1));
        assertEquals(0, result.localTriangles().get(2));
    }

    @ValueSource(ints = {1, 2, 4, 8, 100})
    @ParameterizedTest
    void independentTriangles(int nbrOfTriangles) {
        StringBuilder gdl = new StringBuilder("CREATE ");
        for (int i = 0; i < nbrOfTriangles; ++i) {
            gdl.append(formatWithLocale("(a%d)-[:T]->()-[:T]->()-[:T]->(a%d) ", i, i));
        }

        TriangleCountResult result = compute(fromGdl(gdl.toString()));

        assertEquals(nbrOfTriangles, result.globalTriangles());
        assertEquals(3L * nbrOfTriangles, result.localTriangles().size());
        for (int i = 0; i < result.localTriangles().size(); ++i) {
            assertEquals(1, result.localTriangles().get(i));
        }
    }

    @Test
    void clique5() {
        var graph = fromGdl(
            "CREATE " +
            " (a1)-[:T]->(a2), " +
            " (a1)-[:T]->(a3), " +
            " (a1)-[:T]->(a4), " +
            " (a1)-[:T]->(a5), " +
            " (a2)-[:T]->(a3), " +
            " (a2)-[:T]->(a4), " +
            " (a2)-[:T]->(a5), " +
            " (a3)-[:T]->(a4), " +
            " (a3)-[:T]->(a5), " +
            " (a4)-[:T]->(a5)"
        );

        TriangleCountResult result = compute(graph);

        assertEquals(10, result.globalTriangles());
        assertEquals(5, result.localTriangles().size());
        for (int i = 0; i < result.localTriangles().size(); ++i) {
            assertEquals(6, result.localTriangles().get(i));
        }
    }

    @Test
    void clique5UnionGraph() {
        var graph = fromGdl(
            "CREATE " +
            " (a1)-[:T1]->(a2), " +
            " (a1)-[:T1]->(a3), " +
            " (a1)-[:T2]->(a4), " +
            " (a1)-[:T3]->(a5), " +
            " (a2)-[:T4]->(a3), " +
            " (a2)-[:T2]->(a4), " +
            " (a2)-[:T2]->(a5), " +
            " (a3)-[:T3]->(a4), " +
            " (a3)-[:T1]->(a5), " +
            " (a4)-[:T4]->(a5)"
        );

        TriangleCountResult result = compute(graph);

        assertEquals(10, result.globalTriangles());
        assertEquals(5, result.localTriangles().size());
        for (int i = 0; i < result.localTriangles().size(); ++i) {
            assertEquals(6, result.localTriangles().get(i));
        }
    }

    @Test
    void twoAdjacentTriangles() {
        var graph = fromGdl(
            "CREATE " +
            "  (a)-[:T]->()-[:T]->()-[:T]->(a) " +
            ", (a)-[:T]->()-[:T]->()-[:T]->(a)"
        );

        TriangleCountResult result = compute(graph);

        assertEquals(2, result.globalTriangles());
        assertEquals(5, result.localTriangles().size());
    }

    @Test
    void twoTrianglesWithLine() {
        var graph = fromGdl(
            "CREATE " +
            "  (a)-[:T]->(b)-[:T]->(c)-[:T]->(a) " +
            ", (q)-[:T]->(r)-[:T]->(t)-[:T]->(q) " +
            ", (a)-[:T]->(q)"
        );

        TriangleCountResult result = compute(graph);

        assertEquals(2, result.globalTriangles());
        assertEquals(6, result.localTriangles().size());

        for (int i = 0; i < result.localTriangles().size(); ++i) {
            assertEquals(1, result.localTriangles().get(i));
        }
    }

    @Test
    void selfLoop() {
        var graph = fromGdl("CREATE (a)-[:T]->(a)-[:T]->(a)-[:T]->(a)");

        TriangleCountResult result = compute(graph);

        assertEquals(0, result.globalTriangles());
        assertEquals(1, result.localTriangles().size());
        assertEquals(0, result.localTriangles().get(0));
    }

    @Test
    void selfLoop2() {
        var graph = fromGdl("CREATE (a)-[:T]->(b)-[:T]->(c)-[:T]->(a)-[:T]->(a)");

        TriangleCountResult result = compute(graph);

        assertEquals(1, result.globalTriangles());
        assertEquals(3, result.localTriangles().size());
        assertEquals(1, result.localTriangles().get(0));
        assertEquals(1, result.localTriangles().get(1));
        assertEquals(1, result.localTriangles().get(2));
    }

    @Test
    void parallelRelationships() {
        var graph = fromGdl(
            "CREATE" +
            " (a)-[:T]->(b)-[:T]->(c)-[:T]->(a)" +
            ", (a)-[:T]->(b)"
        );

        TriangleCountResult result = compute(graph);

        assertEquals(1, result.globalTriangles());
        assertEquals(3, result.localTriangles().size());
        assertEquals(1, result.localTriangles().get(0));
        assertEquals(1, result.localTriangles().get(1));
        assertEquals(1, result.localTriangles().get(2));
    }

    @Test
    void parallelTriangles() {
        var graph = fromGdl(
            "CREATE" +
            " (a)-[:T]->(b)-[:T]->(c)-[:T]->(a)" +
            ",(a)-[:T]->(b)-[:T]->(c)-[:T]->(a)"
        );

        TriangleCountResult result = compute(graph);

        assertEquals(1, result.globalTriangles());
        assertEquals(3, result.localTriangles().size());
        assertEquals(1, result.localTriangles().get(0));
        assertEquals(1, result.localTriangles().get(1));
        assertEquals(1, result.localTriangles().get(2));
    }

    @Test
    void triangleNotOnFirstPathAndFirstNodeHasNoMoreNeighbours() {
        var graph = fromGdl(
            "CREATE " +
            "  (n0)-[:REL]->(n1)" +
            ", (n1)-[:REL]->(n2)" +
            ", (n0)-[:REL]->(n3)" +
            ", (n1)-[:REL]->(n3)"
        );

        TriangleCountResult result = compute(graph);

        assertEquals(1, result.globalTriangles());
        assertEquals(4, result.localTriangles().size());
        assertEquals(1, result.localTriangles().get(0));
        assertEquals(1, result.localTriangles().get(1));
        assertEquals(0, result.localTriangles().get(2));
        assertEquals(1, result.localTriangles().get(3));
    }

    @Test
    void triangleNotOnFirstPathAndFirstNodeHasAnotherNeighbours() {
        var graph = fromGdl(
            "CREATE " +
            "  (n0)-[:REL]->(n1)" +
            ", (n1)-[:REL]->(n2)" +
            ", (n0)-[:REL]->(n3)" +
            ", (n0)-[:REL]->(n4)" +
            ", (n1)-[:REL]->(n3)"
        );

        TriangleCountResult result = compute(graph);

        assertEquals(1, result.globalTriangles());
        assertEquals(5, result.localTriangles().size());
        assertEquals(1, result.localTriangles().get(0));
        assertEquals(1, result.localTriangles().get(1));
        assertEquals(0, result.localTriangles().get(2));
        assertEquals(1, result.localTriangles().get(3));
        assertEquals(0, result.localTriangles().get(4));
    }

    @Test
    void triangleNotOnFirstPathAndFirstNodeHasTheMostNeighbours() {
        var graph = fromGdl(
            "CREATE " +
            "  (n0)-[:REL]->(n1)" +
            ", (n1)-[:REL]->(n2)" +
            ", (n0)-[:REL]->(n3)" +
            ", (n0)-[:REL]->(n4)" +
            ", (n0)-[:REL]->(n5)" +
            ", (n1)-[:REL]->(n3)"
        );

        TriangleCountResult result = compute(graph);

        assertEquals(1, result.globalTriangles());
        assertEquals(6, result.localTriangles().size());
        assertEquals(1, result.localTriangles().get(0));
        assertEquals(1, result.localTriangles().get(1));
        assertEquals(0, result.localTriangles().get(2));
        assertEquals(1, result.localTriangles().get(3));
        assertEquals(0, result.localTriangles().get(4));
        assertEquals(0, result.localTriangles().get(5));
    }

    @Test
    void triangleWhenSecondMemberAtEndOfRelChain() {
        var graph = fromGdl(
            "CREATE " +
            "  (n0)-[:REL]->(n1)" +
            ", (n1)-[:REL]->(n2)" +
            ", (n0)-[:REL]->(n3)" +
            ", (n0)-[:REL]->(n4)" +
            ", (n1)-[:REL]->(n4)"
        );

        TriangleCountResult result = compute(graph);

        assertEquals(1, result.globalTriangles());
        assertEquals(5, result.localTriangles().size());
        assertEquals(1, result.localTriangles().get(0));
        assertEquals(1, result.localTriangles().get(1));
        assertEquals(0, result.localTriangles().get(2));
        assertEquals(0, result.localTriangles().get(3));
        assertEquals(1, result.localTriangles().get(4));
    }

    @Test
    void triangleWhenFirstMemberHasMoreNeighbours() {
        var graph = fromGdl(
            "CREATE " +
            "  (n0)-[:REL]->(n1)" +
            ", (n1)-[:REL]->(n2)" +
            ", (n0)-[:REL]->(n3)" +
            ", (n0)-[:REL]->(n4)" +
            ", (n0)-[:REL]->(n5)" +
            ", (n1)-[:REL]->(n4)" +
            ", (n1)-[:REL]->(n6)"
        );

        TriangleCountResult result = compute(graph);

        assertEquals(1, result.globalTriangles());
        assertEquals(7, result.localTriangles().size());
        assertEquals(1, result.localTriangles().get(0));
        assertEquals(1, result.localTriangles().get(1));
        assertEquals(0, result.localTriangles().get(2));
        assertEquals(0, result.localTriangles().get(3));
        assertEquals(1, result.localTriangles().get(4));
        assertEquals(0, result.localTriangles().get(5));
        assertEquals(0, result.localTriangles().get(6));
    }

    @Test
    void filterMaxDegreeFirstCNode() {
        var graph = fromGdl(
            "CREATE " +
            "  (n0)-[:REL]->(n1)" +
            ", (n1)-[:REL]->(n2)" +
            ", (n2)-[:REL]->(n3)" +
            ", (n2)-[:REL]->(n4)" +
            ", (n2)-[:REL]->(n5)" +
            ", (n3)-[:REL]->(n4)" +
            ", (n1)-[:REL]->(n6)" +
            ", (n0)-[:REL]->(n2)" +
            ", (n0)-[:REL]->(n6)"
        );

        TriangleCountBaseConfig config = ImmutableTriangleCountBaseConfig
            .builder()
            .maxDegree(3)
            .build();

        TriangleCountResult result = compute(graph, config);

        assertEquals(1, result.globalTriangles());
        assertEquals(7, result.localTriangles().size());
        assertEquals(1, result.localTriangles().get(0));
        assertEquals(1, result.localTriangles().get(1));
        assertEquals(EXCLUDED_NODE_TRIANGLE_COUNT, result.localTriangles().get(2));
        assertEquals(0, result.localTriangles().get(3));
        assertEquals(0, result.localTriangles().get(4));
        assertEquals(0, result.localTriangles().get(5));
        assertEquals(1, result.localTriangles().get(6));
    }

    @Test
    void filterMaxDegreeSecondCNode() {
        var graph = fromGdl(
            "CREATE " +
            "  (n0)-[:REL]->(n1)" +
            ", (n1)-[:REL]->(n2)" +
            ", (n2)-[:REL]->(n0)" +
            ", (n1)-[:REL]->(n3)" +
            ", (n3)-[:REL]->(n0)" +
            ", (n3)-[:REL]->(n4)" +
            ", (n3)-[:REL]->(n5)" +
            ", (n3)-[:REL]->(n6)"
        );

        TriangleCountBaseConfig config = ImmutableTriangleCountBaseConfig
            .builder()
            .maxDegree(3)
            .build();

        TriangleCountResult result = compute(graph, config);

        assertEquals(1, result.globalTriangles());
        assertEquals(7, result.localTriangles().size());
        assertEquals(1, result.localTriangles().get(0));
        assertEquals(1, result.localTriangles().get(1));
        assertEquals(1, result.localTriangles().get(2));
        assertEquals(EXCLUDED_NODE_TRIANGLE_COUNT, result.localTriangles().get(3));
        assertEquals(0, result.localTriangles().get(4));
        assertEquals(0, result.localTriangles().get(5));
        assertEquals(0, result.localTriangles().get(6));

    }

    @Test
    void manyTrianglesAndOtherThings() {
        var graph = fromGdl(
            "CREATE" +
            " (a)-[:T]->(b)-[:T]->(b)-[:T]->(c)-[:T]->(a)" +
            ", (c)-[:T]->(d)-[:T]->(e)-[:T]->(f)-[:T]->(d)" +
            ", (f)-[:T]->(g)-[:T]->(h)-[:T]->(f)" +
            ", (h)-[:T]->(i)-[:T]->(j)-[:T]->(k)-[:T]->(e)" +
            ", (k)-[:T]->(l)" +
            ", (k)-[:T]->(m)-[:T]->(n)-[:T]->(j)" +
            ", (o)"
        );

        TriangleCountResult result = compute(graph);

        assertEquals(3, result.globalTriangles());
        assertEquals(15, result.localTriangles().size());
        assertEquals(1, result.localTriangles().get(0)); // a
        assertEquals(1, result.localTriangles().get(1)); // b
        assertEquals(1, result.localTriangles().get(2)); // c
        assertEquals(1, result.localTriangles().get(3)); // d
        assertEquals(1, result.localTriangles().get(4)); // e
        assertEquals(2, result.localTriangles().get(5)); // f
        assertEquals(1, result.localTriangles().get(6)); // g
        assertEquals(1, result.localTriangles().get(7)); // h
        assertEquals(0, result.localTriangles().get(8)); // i
        assertEquals(0, result.localTriangles().get(9)); // j
        assertEquals(0, result.localTriangles().get(10)); // k
        assertEquals(0, result.localTriangles().get(11)); // l
        assertEquals(0, result.localTriangles().get(12)); // m
        assertEquals(0, result.localTriangles().get(13)); // n
        assertEquals(0, result.localTriangles().get(14)); // o
    }

    @Test
    void testTriangleCountingWithMaxDegree() {
        var graph = fromGdl(
            "CREATE" +
            "  (a)-[:T]->(b)" +
            " ,(a)-[:T]->(c)" +
            " ,(a)-[:T]->(d)" +
            " ,(b)-[:T]->(c)" +
            " ,(b)-[:T]->(d)" +

            " ,(e)-[:T]->(f)" +
            " ,(f)-[:T]->(g)" +
            " ,(g)-[:T]->(e)"
        );

        TriangleCountBaseConfig config = ImmutableTriangleCountBaseConfig
            .builder()
            .maxDegree(2)
            .build();

        TriangleCountResult result = compute(graph, config);

        assertEquals(EXCLUDED_NODE_TRIANGLE_COUNT, result.localTriangles().get(0)); // a (deg = 3)
        assertEquals(EXCLUDED_NODE_TRIANGLE_COUNT, result.localTriangles().get(1)); // b (deg = 3)
        assertEquals(0, result.localTriangles().get(2));  // c (deg = 2)
        assertEquals(0, result.localTriangles().get(3));  // d (deg = 2)

        assertEquals(1, result.localTriangles().get(4)); // e (deg = 2)
        assertEquals(1, result.localTriangles().get(5)); // f (deg = 2)
        assertEquals(1, result.localTriangles().get(6)); // g (deg = 2)
        assertEquals(1, result.globalTriangles());
    }

    @Test
    void testTriangleCountingWithMaxDegreeOnUnionGraph() {
        var graph = fromGdl(
            "CREATE" +
            "  (a)-[:T1]->(b)" +
            " ,(a)-[:T2]->(c)" +
            " ,(a)-[:T2]->(d)" +
            " ,(b)-[:T1]->(c)" +
            " ,(b)-[:T2]->(d)" +

            " ,(e)-[:T1]->(f)" +
            " ,(f)-[:T1]->(g)" +
            " ,(g)-[:T1]->(e)"
        );

        TriangleCountBaseConfig config = ImmutableTriangleCountBaseConfig
            .builder()
            .maxDegree(2)
            .build();

        TriangleCountResult result = compute(graph, config);

        assertEquals(EXCLUDED_NODE_TRIANGLE_COUNT, result.localTriangles().get(0)); // a (deg = 3)
        assertEquals(EXCLUDED_NODE_TRIANGLE_COUNT, result.localTriangles().get(1)); // b (deg = 3)
        assertEquals(0, result.localTriangles().get(2));  // c (deg = 2)
        assertEquals(0, result.localTriangles().get(3));  // d (deg = 2)

        assertEquals(1, result.localTriangles().get(4)); // e (deg = 2)
        assertEquals(1, result.localTriangles().get(5)); // f (deg = 2)
        assertEquals(1, result.localTriangles().get(6)); // g (deg = 2)
        assertEquals(1, result.globalTriangles());
    }

    @Test
    void testTriangleCountingOnUnionGraphWithIncompleteTriangles() {
        // triangle would be (a)-(b)-(c), but it is not complete, there is no (b)-(c)
        // (b) is connected to (x) and (y), both have smaller ids than (c)
        // TC tries to find (b)-(c) und the union graph has to exhaust all cursors during advance
        // to learn that there are only nodes that are smaller than (c)
        var testGraph = TestSupport.fromGdl(
            "CREATE" +
            "  (a)-[:T]->(b)" +
            " ,(b)-[:X]->(x)" +
            " ,(b)-[:Y]->(y)" +
            " ,(a)-[:T]->(c)",
            UNDIRECTED
        );

        var config = ImmutableTriangleCountBaseConfig.builder().build();
        var result = compute(testGraph.graph(), config);

        assertThat(result.globalTriangles()).isEqualTo(0L);
        assertThat(result.localTriangles())
            .returns(0L, t -> t.get(testGraph.toMappedNodeId("a")))
            .returns(0L, t -> t.get(testGraph.toMappedNodeId("b")))
            .returns(0L, t -> t.get(testGraph.toMappedNodeId("c")))
            .returns(0L, t -> t.get(testGraph.toMappedNodeId("x")))
            .returns(0L, t -> t.get(testGraph.toMappedNodeId("y")));
    }

    private TriangleCountResult compute(Graph graph) {
        TriangleCountStatsConfig config = ImmutableTriangleCountStatsConfig.builder().build();
        return compute(graph, config);
    }

    private TriangleCountResult compute(Graph graph, TriangleCountBaseConfig config) {
        return IntersectingTriangleCount.create(graph, config, Pools.DEFAULT).compute();
    }

    private static Graph fromGdl(String gdl) {
        return TestSupport.fromGdl(gdl, Orientation.UNDIRECTED).graph();
    }
}
