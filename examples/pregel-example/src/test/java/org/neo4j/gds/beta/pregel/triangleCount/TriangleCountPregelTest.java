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
package org.neo4j.gds.beta.pregel.triangleCount;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.beta.pregel.Pregel;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.ProgressLogger;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.paged.HugeLongArray;

import java.util.Arrays;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.gds.Orientation.UNDIRECTED;
import static org.neo4j.gds.TestSupport.fromGdl;

class TriangleCountPregelTest {

    private static Stream<Arguments> noTriangleQueries() {
        return Stream.of(
            Arguments.of(fromGdl("CREATE ()-[:T]->()-[:T]->()", UNDIRECTED), "line"),
            Arguments.of(fromGdl("CREATE (), (), ()", UNDIRECTED), "no rels"),
            Arguments.of(fromGdl("CREATE ()-[:T]->(), ()", UNDIRECTED), "one rel"),
            Arguments.of(fromGdl("CREATE (a1)-[:T]->()-[:T]->(a1), ()", UNDIRECTED), "back and forth")
        );
    }

    @MethodSource("noTriangleQueries")
    @ParameterizedTest(name = "{1}")
    void noTriangles(Graph graph, String ignoredName) {
        HugeLongArray result = nodeWiseTriangles(graph);

        assertEquals(0L, globalCount(result));
        assertEquals(3, result.size());
        assertEquals(0, result.get(0));
        assertEquals(0, result.get(1));
        assertEquals(0, result.get(2));
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
            " (a4)-[:T]->(a5)",
            UNDIRECTED
        );

        HugeLongArray result = nodeWiseTriangles(graph);

        assertEquals(10, globalCount(result));
        assertEquals(5, result.size());
        for (int i = 0; i < result.size(); ++i) {
            assertEquals(6, result.get(i));
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
            " (a4)-[:T4]->(a5)",
            UNDIRECTED
        );

        HugeLongArray result = nodeWiseTriangles(graph);

        assertEquals(10, globalCount(result));
        assertEquals(5, result.size());
        for (int i = 0; i < result.size(); ++i) {
            assertEquals(6, result.get(i));
        }
    }

    @Test
    void twoAdjacentTriangles() {
        var graph = fromGdl(
            "CREATE " +
            "  (a)-[:T]->()-[:T]->()-[:T]->(a) " +
            ", (a)-[:T]->()-[:T]->()-[:T]->(a)",
            UNDIRECTED
        );

        HugeLongArray result = nodeWiseTriangles(graph);

        assertEquals(2, globalCount(result));
        assertEquals(5, result.size());
    }

    @Test
    void twoTrianglesWithLine() {
        var graph = fromGdl(
            "CREATE " +
            "  (a)-[:T]->(b)-[:T]->(c)-[:T]->(a) " +
            ", (q)-[:T]->(r)-[:T]->(t)-[:T]->(q) " +
            ", (a)-[:T]->(q)",
            UNDIRECTED
        );

        HugeLongArray result = nodeWiseTriangles(graph);

        assertEquals(2, globalCount(result));
        assertEquals(6, result.size());

        for (int i = 0; i < result.size(); ++i) {
            assertEquals(1, result.get(i));
        }
    }

    @Test
    void selfLoop() {
        var graph = fromGdl("CREATE (a)-[:T]->(a)-[:T]->(a)-[:T]->(a)", UNDIRECTED);

        HugeLongArray result = nodeWiseTriangles(graph);

        assertEquals(0, globalCount(result));
        assertEquals(1, result.size());
        assertEquals(0, result.get(0));
    }

    @Test
    void selfLoop2() {
        var graph = fromGdl("CREATE (a)-[:T]->(b)-[:T]->(c)-[:T]->(a)-[:T]->(a)", UNDIRECTED);

        HugeLongArray result = nodeWiseTriangles(graph);

        assertEquals(1, globalCount(result));
        assertEquals(3, result.size());
        assertEquals(1, result.get(0));
        assertEquals(1, result.get(1));
        assertEquals(1, result.get(2));
    }

    @Test
    void parallelRelationships() {
        var graph = fromGdl(
            "CREATE" +
            " (a)-[:T]->(b)-[:T]->(c)-[:T]->(a)" +
            ", (a)-[:T]->(b)",
            UNDIRECTED
        );

        HugeLongArray result = nodeWiseTriangles(graph);

        assertEquals(1, globalCount(result));
        assertEquals(3, result.size());
        assertEquals(1, result.get(0));
        assertEquals(1, result.get(1));
        assertEquals(1, result.get(2));
    }

    @Test
    void parallelTriangles() {
        var graph = fromGdl(
            "CREATE" +
            " (a)-[:T]->(b)-[:T]->(c)-[:T]->(a)" +
            ",(a)-[:T]->(b)-[:T]->(c)-[:T]->(a)",
            UNDIRECTED
        );

        HugeLongArray result = nodeWiseTriangles(graph);

        assertEquals(1, globalCount(result));
        assertEquals(3, result.size());
        assertEquals(1, result.get(0));
        assertEquals(1, result.get(1));
        assertEquals(1, result.get(2));
    }

    @Test
    void triangleNotOnFirstPathAndFirstNodeHasNoMoreNeighbours() {
        var graph = fromGdl(
            "CREATE " +
            "  (n0)-[:REL]->(n1)" +
            ", (n1)-[:REL]->(n2)" +
            ", (n0)-[:REL]->(n3)" +
            ", (n1)-[:REL]->(n3)",
            UNDIRECTED
        );

        HugeLongArray result = nodeWiseTriangles(graph);

        assertEquals(1, globalCount(result));
        assertEquals(4, result.size());
        assertEquals(1, result.get(0));
        assertEquals(1, result.get(1));
        assertEquals(0, result.get(2));
        assertEquals(1, result.get(3));
    }

    @Test
    void triangleNotOnFirstPathAndFirstNodeHasAnotherNeighbours() {
        var graph = fromGdl(
            "CREATE " +
            "  (n0)-[:REL]->(n1)" +
            ", (n1)-[:REL]->(n2)" +
            ", (n0)-[:REL]->(n3)" +
            ", (n0)-[:REL]->(n4)" +
            ", (n1)-[:REL]->(n3)",
            UNDIRECTED
        );

        HugeLongArray result = nodeWiseTriangles(graph);

        assertEquals(1, globalCount(result));
        assertEquals(5, result.size());
        assertEquals(1, result.get(0));
        assertEquals(1, result.get(1));
        assertEquals(0, result.get(2));
        assertEquals(1, result.get(3));
        assertEquals(0, result.get(4));
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
            ", (n1)-[:REL]->(n3)",
            UNDIRECTED
        );

        HugeLongArray result = nodeWiseTriangles(graph);

        assertEquals(1, globalCount(result));
        assertEquals(6, result.size());
        assertEquals(1, result.get(0));
        assertEquals(1, result.get(1));
        assertEquals(0, result.get(2));
        assertEquals(1, result.get(3));
        assertEquals(0, result.get(4));
        assertEquals(0, result.get(5));
    }

    @Test
    void triangleWhenSecondMemberAtEndOfRelChain() {
        var graph = fromGdl(
            "CREATE " +
            "  (n0)-[:REL]->(n1)" +
            ", (n1)-[:REL]->(n2)" +
            ", (n0)-[:REL]->(n3)" +
            ", (n0)-[:REL]->(n4)" +
            ", (n1)-[:REL]->(n4)",
            UNDIRECTED
        );

        HugeLongArray result = nodeWiseTriangles(graph);

        assertEquals(1, globalCount(result));
        assertEquals(5, result.size());
        assertEquals(1, result.get(0));
        assertEquals(1, result.get(1));
        assertEquals(0, result.get(2));
        assertEquals(0, result.get(3));
        assertEquals(1, result.get(4));
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
            ", (n1)-[:REL]->(n6)",
            UNDIRECTED
        );

        HugeLongArray result = nodeWiseTriangles(graph);

        assertEquals(1, globalCount(result));
        assertEquals(7, result.size());
        assertEquals(1, result.get(0));
        assertEquals(1, result.get(1));
        assertEquals(0, result.get(2));
        assertEquals(0, result.get(3));
        assertEquals(1, result.get(4));
        assertEquals(0, result.get(5));
        assertEquals(0, result.get(6));
    }

    HugeLongArray nodeWiseTriangles(Graph graph) {
        var config = ImmutableTriangleCountPregelConfig.builder().maxIterations(4).build();

        var pregelJob = Pregel.create(
            graph,
            config,
            new TriangleCountPregel(),
            Pools.DEFAULT,
            AllocationTracker.empty(),
            ProgressLogger.NULL_LOGGER
        );

        return pregelJob.run().nodeValues().longProperties(TriangleCountPregel.TRIANGLE_COUNT);
    }
    
    long globalCount(HugeLongArray nodeWiseCounts) {
        return Arrays.stream(nodeWiseCounts.toArray()).sum() / 3;
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
            ", (o)",
            UNDIRECTED
        );

        HugeLongArray result = nodeWiseTriangles(graph);

        assertEquals(3, globalCount(result));
        assertEquals(15, result.size());
        assertEquals(1, result.get(0)); // a
        assertEquals(1, result.get(1)); // b
        assertEquals(1, result.get(2)); // c
        assertEquals(1, result.get(3)); // d
        assertEquals(1, result.get(4)); // e
        assertEquals(2, result.get(5)); // f
        assertEquals(1, result.get(6)); // g
        assertEquals(1, result.get(7)); // h
        assertEquals(0, result.get(8)); // i
        assertEquals(0, result.get(9)); // j
        assertEquals(0, result.get(10)); // k
        assertEquals(0, result.get(11)); // l
        assertEquals(0, result.get(12)); // m
        assertEquals(0, result.get(13)); // n
        assertEquals(0, result.get(14)); // o
    }
}
