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
package org.neo4j.gds.cliqueCounting;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.TestSupport;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.cliquecounting.CliqueCountingMode;
import org.neo4j.gds.cliquecounting.CliqueCountingParameters;
import org.neo4j.gds.collections.ha.HugeObjectArray;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.termination.TerminationFlag;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CliqueCountingTest {

    private static Graph fromGdl(String gdl) {
        return TestSupport.fromGdl(gdl, Orientation.UNDIRECTED).graph();
    }

    private static Stream<Arguments> cliqueOfFourQuery() {
        return Stream.of(
            Arguments.of(
                fromGdl("CREATE (a1)-[:T]->(a2)-[:T]->(a3)-[:T]->(a4), (a2)-[:T]->(a4)-[:T]->(a1)-[:T]->(a3)"),
                "fourClique"
            )
        );
    }

    private static Stream<Arguments> cliqueOfFourWithDuplicateEdgesQuery() {
        return Stream.of(
            Arguments.of(
                fromGdl("CREATE (a1)-[:T]->(a2)-[:T]->(a3)-[:T]->(a4), (a2)-[:T]->(a4)-[:T]->(a1)-[:T]->(a3), (a1)-[:T]->(a2), (a1)-[:T1]->(a3)"),
                "fourClique"
            )
        );
    }

//    public void testPartitionSubset() {
//        var graph = fromGdl("CREATE (a), (b), (c), (d), (b)-[:T]->(c)-[:T]->(d), (d)-[:T]->(b)");
//        long[] subset = {1, 2, 3};
//        graph.forEachRelationship(
//            0, (a, b) -> {
//                System.out.println(a + " " + b);
//                return true;
//            }
//        );
//        var cliqueCounting = CliqueCounting.create(
//            graph,
//            new CliqueCountingParameters(
//                CliqueCountingMode.ForEveryNode,
//                List.of(),
//                new Concurrency(1)
//            ),
//            DefaultPool.INSTANCE,
//            ProgressTracker.NULL_TRACKER,
//            TerminationFlag.RUNNING_TRUE
//        );
////        var count = cliqueCounting.neighborhoodIntersectionSize(graph, subset, 0L);
//        int[] intersectionSizes = {2, 2, 2};
//        var partition = cliqueCounting.partitionSubset(subset);
////        assertEquals(1, count);
//        System.out.println(partition);
//    }

    private static Stream<Arguments> starAndTrianglesQuery() {
        return Stream.of(
            Arguments.of(
                fromGdl("CREATE " +
                    "(a0)-[:T]->(a1), " +
                    "(a0)-[:T]->(a2), " +
                    "(a0)-[:T]->(a3), " +
                    "(a0)-[:T]->(a4), " +
                    "(a0)-[:T]->(a5), " +
                    "(a0)-[:T]->(a6), " +
                    "(a1)-[:T]->(a2)-[:T]->(a3)-[:T]->(a1), " + //Triangle + centre
                    "(a4)-[:T]->(a5)-[:T]->(a6)-[:T]->(a4), "   //Triangle + centre
                ), "starAndTriangles"
            )
        );
    }

    private CliqueCountingResult compute(Graph graph, int concurrency, HugeObjectArray<long[]> subcliques) {
        var cliqueCountingMode = subcliques.size() == 0
            ? CliqueCountingMode.ForEveryNode
            : CliqueCountingMode.ForGivenSubcliques;
        return CliqueCounting.create(
            graph,
            new CliqueCountingParameters(
                cliqueCountingMode,
                Arrays.stream(subcliques.toArray()).toList(),
                new Concurrency(concurrency)
            ),
            DefaultPool.INSTANCE,
            ProgressTracker.NULL_TRACKER,
            TerminationFlag.RUNNING_TRUE
        ).compute();
    }

    @MethodSource("cliqueOfFourQuery")
    @ParameterizedTest(name = "{1}")
    void cliqueOfFour(Graph graph, String ignoredName) {
        CliqueCountingResult result = compute(graph, 4, HugeObjectArray.of());

        assertEquals(5 - 3, result.globalCount().length);
        assertEquals(4L, result.globalCount()[3 - 3]);
        assertEquals(1L, result.globalCount()[4 - 3]);

        assertEquals(2L, result.perNodeCount().get(1L).length);
        assertEquals(3L, result.perNodeCount().get(1L)[3 - 3]);
        assertEquals(1L, result.perNodeCount().get(1L)[4 - 3]);

    }

    @MethodSource("cliqueOfFourWithDuplicateEdgesQuery")
    @ParameterizedTest(name = "{1}")
    void duplicateEdges(Graph graph, String ignoredName) {
        CliqueCountingResult result = compute(graph, 4, HugeObjectArray.of());

        assertEquals(5 - 3, result.globalCount().length);
        assertEquals(4L, result.globalCount()[3 - 3]);
        assertEquals(1L, result.globalCount()[4 - 3]);

        assertEquals(2L, result.perNodeCount().get(1L).length);
        assertEquals(3L, result.perNodeCount().get(1L)[3 - 3]);
        assertEquals(1L, result.perNodeCount().get(1L)[4 - 3]);

    }

    @MethodSource("starAndTrianglesQuery")
    @ParameterizedTest(name = "{1}")
    void globalAndPerNodeCount(Graph graph, String ignoredName) {
        CliqueCountingResult result1 = compute(graph, 4, HugeObjectArray.of());
        assertEquals(2L, result1.globalCount().length);
        assertEquals(8L, result1.globalCount()[3 - 3]);
        assertEquals(2L, result1.globalCount()[4 - 3]);

        assertEquals(2L, result1.perNodeCount().get(0L).length);
        assertEquals(6L, result1.perNodeCount().get(0L)[3 - 3]);
        assertEquals(2L, result1.perNodeCount().get(0L)[4 - 3]);
    }



    @MethodSource("starAndTrianglesQuery")
    @ParameterizedTest(name = "{1}")
    void subcliqueCount(Graph graph, String ignoredName) {
        long[] subclique1 = {0, 1, 2};
        long[] subclique2 = {0, 4};
        CliqueCountingResult result2 = compute(graph, 4, HugeObjectArray.of(subclique1, subclique2));
        assertEquals(2L, result2.perSubcliqueCount()[0].length);
        assertEquals(1L, result2.perSubcliqueCount()[0][3 - 3]);
        assertEquals(1L, result2.perSubcliqueCount()[0][4 - 3]);

        assertEquals(2L, result2.perSubcliqueCount()[1].length);
        assertEquals(2L, result2.perSubcliqueCount()[1][3 - 3]);
        assertEquals(1L, result2.perSubcliqueCount()[1][4 - 3]);
    }

    @Disabled
    void bigGraph() throws IOException {
        //Expected: [38542, 31170, 17387, 6306, 1273, 114, 1]
        String gdlString = Files.readString(Path.of("/Users/alfred/graph-analytics/public/algo/src/test/java/org/neo4j/gds/cliqueCounting/gdl_n1k_m20_p0.05.txt"));
        Graph graph = fromGdl(gdlString);
        CliqueCountingResult result = compute(graph, 16, HugeObjectArray.of());
        assertEquals(38542L, result.globalCount()[3 - 3]);
        assertEquals(31170L, result.globalCount()[4 - 3]);
        assertEquals(17387L, result.globalCount()[5 - 3]);
        assertEquals(6306L, result.globalCount()[6 - 3]);
        assertEquals(1273L, result.globalCount()[7 - 3]);
        assertEquals(114L, result.globalCount()[8 - 3]);
        assertEquals(1L, result.globalCount()[9 - 3]);
        System.out.println(Arrays.toString(result.globalCount()));
    }

    @Disabled
    void biggerGraph() throws IOException {
        //Expected:
        String gdlString = Files.readString(Path.of("/Users/alfred/graph-analytics/public/algo/src/test/java/org/neo4j/gds/cliqueCounting/gdl_n10k_m25_p0.05.txt"));
        Graph graph = fromGdl(gdlString);

        CliqueCountingResult result = compute(graph, 16, HugeObjectArray.of());
        var start = System.nanoTime();
        for (int i = 0; i < 10; i++) {
            result = compute(graph, 16, HugeObjectArray.of());
        }
        var end = System.nanoTime();
        System.out.println("Avg execution time: " + (end - start)/10_000_000 + "ms");
//        System.out.println(Arrays.toString(result.globalCount()));
    }

}
