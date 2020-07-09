/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.triangle;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.graphalgo.AlgoTestBase;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.StoreLoaderBuilder;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.triangle.IntersectingTriangleCount.TriangleCountResult;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.graphalgo.triangle.IntersectingTriangleCount.EXCLUDED_NODE_TRIANGLE_COUNT;

class IntersectingTriangleCountTest extends AlgoTestBase {

    private static Stream<Arguments> noTriangleQueries() {
        return Stream.of(
            Arguments.of("CREATE ()-[:T]->()-[:T]->()", "line"),
            Arguments.of("CREATE (), (), ()", "no rels"),
            Arguments.of("CREATE ()-[:T]->(), ()", "one rel"),
            Arguments.of("CREATE (a1)-[:T]->()-[:T]->(a1), ()", "back and forth")
        );
    }

    @MethodSource("noTriangleQueries")
    @ParameterizedTest(name = "{1}")
    void noTriangles(String query, String ignoredName) {
        runQuery(query);

        TriangleCountResult result =  projectAndCompute();

        assertEquals(0L, result.globalTriangles());
        assertEquals(3, result.localTriangles().size());
        assertEquals(0, result.localTriangles().get(0));
        assertEquals(0, result.localTriangles().get(1));
        assertEquals(0, result.localTriangles().get(2));
    }

    @ValueSource(ints = {1, 2, 4, 8, 100})
    @ParameterizedTest
    void independentTriangles(int nbrOfTriangles) {
        for (int i = 0; i < nbrOfTriangles; ++i) {
            runQuery("CREATE (a)-[:T]->(b)-[:T]->(c)-[:T]->(a)");
        }

        TriangleCountResult result =  projectAndCompute();

        assertEquals(nbrOfTriangles, result.globalTriangles());
        assertEquals(3 * nbrOfTriangles, result.localTriangles().size());
        for (int i = 0; i < result.localTriangles().size(); ++i) {
            assertEquals(1, result.localTriangles().get(i));
        }
    }

    @Test
    void clique5() {
        runQuery("CREATE (a1), (a2), (a3), (a4), (a5) " +
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
                 " (a4)-[:T]->(a5)");

        TriangleCountResult result =  projectAndCompute();

        assertEquals(10, result.globalTriangles());
        assertEquals(5, result.localTriangles().size());
        for (int i = 0; i < result.localTriangles().size(); ++i) {
            assertEquals(6, result.localTriangles().get(i));
        }
    }

    @Test
    void twoAdjacentTriangles() {
        runQuery("CREATE (a)-[:T]->(b)-[:T]->(c)-[:T]->(a) " +
                 "CREATE (a)-[:T]->(r)-[:T]->(t)-[:T]->(a)");

        TriangleCountResult result =  projectAndCompute();

        assertEquals(2, result.globalTriangles());
        assertEquals(5, result.localTriangles().size());
    }

    @Test
    void twoTrianglesWithLine() {
        runQuery("CREATE (a)-[:T]->(b)-[:T]->(c)-[:T]->(a) " +
                 "CREATE (q)-[:T]->(r)-[:T]->(t)-[:T]->(q) " +
                 "CREATE (a)-[:T]->(q)");

        TriangleCountResult result =  projectAndCompute();

        assertEquals(2, result.globalTriangles());
        assertEquals(6, result.localTriangles().size());

        for (int i = 0; i < result.localTriangles().size(); ++i) {
            assertEquals(1, result.localTriangles().get(i));
        }
    }

    @Test
    void selfLoop() {
        runQuery("CREATE (a)-[:T]->(a)-[:T]->(a)-[:T]->(a)");

        TriangleCountResult result = projectAndCompute();

        assertEquals(0, result.globalTriangles());
        assertEquals(1, result.localTriangles().size());
        assertEquals(0, result.localTriangles().get(0));
    }

    @Test
    void selfLoop2() {
        runQuery("CREATE (a)-[:T]->(b)-[:T]->(c)-[:T]->(a)-[:T]->(a)");

        TriangleCountResult result = projectAndCompute();

        assertEquals(1, result.globalTriangles());
        assertEquals(3, result.localTriangles().size());
        assertEquals(1, result.localTriangles().get(0));
        assertEquals(1, result.localTriangles().get(1));
        assertEquals(1, result.localTriangles().get(2));
    }

    @Test
    void parallelRelationships() {
        runQuery(
            "CREATE" +
            " (a)-[:T]->(b)-[:T]->(c)-[:T]->(a)" +
            ", (a)-[:T]->(b)"
        );

        TriangleCountResult result = projectAndCompute();

        assertEquals(1, result.globalTriangles());
        assertEquals(3, result.localTriangles().size());
        assertEquals(1, result.localTriangles().get(0));
        assertEquals(1, result.localTriangles().get(1));
        assertEquals(1, result.localTriangles().get(2));
    }

    @Test
    void parallelTriangles() {
        runQuery(
            "CREATE" +
            " (a)-[:T]->(b)-[:T]->(c)-[:T]->(a)" +
            ",(a)-[:T]->(b)-[:T]->(c)-[:T]->(a)"
        );

        TriangleCountResult result = projectAndCompute();

        assertEquals(1, result.globalTriangles());
        assertEquals(3, result.localTriangles().size());
        assertEquals(1, result.localTriangles().get(0));
        assertEquals(1, result.localTriangles().get(1));
        assertEquals(1, result.localTriangles().get(2));
    }

    @Test
    void manyTrianglesAndOtherThings() {
        runQuery("CREATE" +
                 " (a)-[:T]->(b)-[:T]->(b)-[:T]->(c)-[:T]->(a), " +
                 " (c)-[:T]->(d)-[:T]->(e)-[:T]->(f)-[:T]->(d), " +
                 " (f)-[:T]->(g)-[:T]->(h)-[:T]->(f), " +
                 " (h)-[:T]->(i)-[:T]->(j)-[:T]->(k)-[:T]->(e), " +
                 " (k)-[:T]->(l), " +
                 " (k)-[:T]->(m)-[:T]->(n)-[:T]->(j), " +
                 " (o)");

        TriangleCountResult result = projectAndCompute();

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
        runQuery(
            "CREATE" +
            "  (a)-[:T]->(b)"+
            " ,(a)-[:T]->(c)"+
            " ,(a)-[:T]->(d)"+
            " ,(b)-[:T]->(c)"+
            " ,(b)-[:T]->(d)"+

            " ,(e)-[:T]->(f)"+
            " ,(f)-[:T]->(g)"+
            " ,(g)-[:T]->(e)"
        );

        TriangleCountBaseConfig config = ImmutableTriangleCountBaseConfig
            .builder()
            .maxDegree(2)
            .build();

        TriangleCountResult result = projectAndCompute(config);

        assertEquals(EXCLUDED_NODE_TRIANGLE_COUNT, result.localTriangles().get(0)); // a (deg = 3)
        assertEquals(EXCLUDED_NODE_TRIANGLE_COUNT, result.localTriangles().get(1)); // b (deg = 3)
        assertEquals(0, result.localTriangles().get(2));  // c (deg = 2)
        assertEquals(0, result.localTriangles().get(3));  // d (deg = 2)

        assertEquals(1, result.localTriangles().get(4)); // e (deg = 2)
        assertEquals(1, result.localTriangles().get(5)); // f (deg = 2)
        assertEquals(1, result.localTriangles().get(6)); // g (deg = 2)
        assertEquals(1, result.globalTriangles());
    }

    private TriangleCountResult projectAndCompute() {
        TriangleCountStatsConfig config = ImmutableTriangleCountStatsConfig.builder().build();
        return projectAndCompute(config);
    }

    private TriangleCountResult projectAndCompute(TriangleCountBaseConfig config) {
        Graph graph =  new StoreLoaderBuilder()
            .api(db)
            .globalOrientation(Orientation.UNDIRECTED)
            .build()
            .graph();

        return new IntersectingTriangleCount(
            graph,
            config,
            Pools.DEFAULT,
            AllocationTracker.EMPTY
        ).compute();
    }
}
