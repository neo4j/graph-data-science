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
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

// TODO: Add tests for seeded property
class LocalClusteringCoefficientTest extends AlgoTestBase {

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

        LocalClusteringCoefficient.Result result = projectWithoutSeedAndCompute();

        assertEquals(0, result.averageClusteringCoefficient());
        assertEquals(3, result.localClusteringCoefficients().size());
        assertEquals(0.0, result.localClusteringCoefficients().get(0));
        assertEquals(0.0, result.localClusteringCoefficients().get(1));
        assertEquals(0.0, result.localClusteringCoefficients().get(2));
    }

    @ValueSource(ints = {1, 2, 4, 8, 100})
    @ParameterizedTest
    void independentTriangles(int nbrOfTriangles) {
        for (int i = 0; i < nbrOfTriangles; ++i) {
            runQuery("CREATE (a)-[:T]->(b)-[:T]->(c)-[:T]->(a)");
        }

        LocalClusteringCoefficient.Result result = projectWithoutSeedAndCompute();

        assertEquals(1, result.averageClusteringCoefficient());
        assertEquals(3 * nbrOfTriangles, result.localClusteringCoefficients().size());
        for (int i = 0; i < result.localClusteringCoefficients().size(); ++i) {
            assertEquals(1.0, result.localClusteringCoefficients().get(i));
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

        LocalClusteringCoefficient.Result result = projectWithoutSeedAndCompute();

        assertEquals(1, result.averageClusteringCoefficient());
        assertEquals(5, result.localClusteringCoefficients().size());
        for (int i = 0; i < result.localClusteringCoefficients().size(); ++i) {
            assertEquals(1.0, result.localClusteringCoefficients().get(i));
        }
    }

    @Test
    void twoAdjacentTriangles() {
        runQuery("CREATE (a)-[:T]->(b)-[:T]->(c)-[:T]->(a) " +
                 "CREATE (a)-[:T]->(r)-[:T]->(t)-[:T]->(a)");

        LocalClusteringCoefficient.Result result = projectWithoutSeedAndCompute();

        assertEquals(13.0 / 15.0, result.averageClusteringCoefficient(), 1e-10);
        assertEquals(5, result.localClusteringCoefficients().size());


        Collection<Double> localCCs = new ArrayList<>();
        for (int i = 0; i < result.localClusteringCoefficients().size(); ++i) {
            localCCs.add(result.localClusteringCoefficients().get(i));
        }

        Map<Double, Long> groupedCcs = localCCs
            .stream()
            .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
        assertEquals(4, groupedCcs.get(1.0));
        assertEquals(1, groupedCcs.get(1.0 / 3));
    }

    @Test
    void twoTrianglesWithLine() {
        runQuery("CREATE (a)-[:T]->(b)-[:T]->(c)-[:T]->(a) " +
                 "CREATE (q)-[:T]->(r)-[:T]->(t)-[:T]->(q) " +
                 "CREATE (a)-[:T]->(q)");

        LocalClusteringCoefficient.Result result = projectWithoutSeedAndCompute();

        assertEquals(7.0 / 9.0, result.averageClusteringCoefficient(), 1e-10);
        assertEquals(6, result.localClusteringCoefficients().size());

        Collection<Double> localCCs = new ArrayList<>();
        for (int i = 0; i < result.localClusteringCoefficients().size(); ++i) {
            localCCs.add(result.localClusteringCoefficients().get(i));
        }

        Map<Double, Long> groupedCcs = localCCs
            .stream()
            .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
        assertEquals(4, groupedCcs.get(1.0));
        assertEquals(2, groupedCcs.get(1.0 / 3));
    }

    @Test
    void selfLoop() {
        runQuery("CREATE (a)-[:T]->(a)-[:T]->(a)-[:T]->(a)");

        LocalClusteringCoefficient.Result result = projectWithoutSeedAndCompute();

        assertEquals(0, result.averageClusteringCoefficient());
        assertEquals(1, result.localClusteringCoefficients().size());
        assertEquals(0.0, result.localClusteringCoefficients().get(0));
    }

    @Test
    void triangleWithSelfLoop() {
        // a self loop adds one to the degree
        runQuery("CREATE (a)-[:T]->(b)-[:T]->(c)-[:T]->(a)-[:T]->(a)");

        LocalClusteringCoefficient.Result result = projectWithoutSeedAndCompute();

        assertEquals(7.0 / 9, result.averageClusteringCoefficient(), 1e-10);
        assertEquals(3, result.localClusteringCoefficients().size());
        assertEquals(1.0 / 3, result.localClusteringCoefficients().get(0));
        assertEquals(1.0, result.localClusteringCoefficients().get(1));
        assertEquals(1.0, result.localClusteringCoefficients().get(2));
    }

    @Test
    void triangleWithParallelRelationship() {
        runQuery(
            "CREATE" +
            " (a)-[:T]->(b)-[:T]->(c)-[:T]->(a)" +
            ",(a)-[:T]->(b)"
        );

        LocalClusteringCoefficient.Result result = projectWithoutSeedAndCompute();

        assertEquals(1.0, result.averageClusteringCoefficient());
        assertEquals(3, result.localClusteringCoefficients().size());
        assertEquals(1, result.localClusteringCoefficients().get(0));
        assertEquals(1, result.localClusteringCoefficients().get(1));
        assertEquals(1, result.localClusteringCoefficients().get(2));
    }

    @Test
    void triangleWithTwoParallelRelationships() {
        runQuery(
            "CREATE" +
            " (a)-[:T]->(b)-[:T]->(c)-[:T]->(a)" +
            ",(a)-[:T]->(b)-[:T]->(c)"
        );

        LocalClusteringCoefficient.Result result = projectWithoutSeedAndCompute();

        assertEquals(1.0, result.averageClusteringCoefficient());
        assertEquals(3, result.localClusteringCoefficients().size());
        assertEquals(1, result.localClusteringCoefficients().get(0));
        assertEquals(1, result.localClusteringCoefficients().get(1));
        assertEquals(1, result.localClusteringCoefficients().get(2));
    }

    @Test
    void parallelTriangles() {
        runQuery(
            "CREATE" +
            " (a)-[:T]->(b)-[:T]->(c)-[:T]->(a)" +
            ",(a)-[:T]->(b)-[:T]->(c)-[:T]->(a)"
        );

        LocalClusteringCoefficient.Result result = projectWithoutSeedAndCompute();

        assertEquals(1.0, result.averageClusteringCoefficient());
        assertEquals(3, result.localClusteringCoefficients().size());
        assertEquals(1, result.localClusteringCoefficients().get(0));
        assertEquals(1, result.localClusteringCoefficients().get(1));
        assertEquals(1, result.localClusteringCoefficients().get(2));
    }

    @Test
    void parallelTrianglesWithExtraParallelRelationship() {
        runQuery(
            "CREATE" +
            " (a)-[:T]->(b)-[:T]->(c)-[:T]->(a)" +
            ",(a)-[:T]->(b)-[:T]->(c)-[:T]->(a)" +
            ",(a)-[:T]->(b)"
        );

        LocalClusteringCoefficient.Result result = projectWithoutSeedAndCompute();

        assertEquals(1.0, result.averageClusteringCoefficient());
        assertEquals(3, result.localClusteringCoefficients().size());
        assertEquals(1, result.localClusteringCoefficients().get(0));
        assertEquals(1, result.localClusteringCoefficients().get(1));
        assertEquals(1, result.localClusteringCoefficients().get(2));
    }

    @Test
    void triangleWithParallelRelationshipAndExtraNode() {
        runQuery(
            "CREATE" +
            " (a)-[:T]->(b)-[:T]->(c)-[:T]->(a)" +
            ",(a)-[:T]->(b)" +
            ",(d)-[:T]->(a)"
        );

        LocalClusteringCoefficient.Result result = projectWithoutSeedAndCompute();

        assertEquals(4, result.localClusteringCoefficients().size());
        assertEquals(7.0 / 12, result.averageClusteringCoefficient(), 1e-10);
        assertEquals(1.0 / 3, result.localClusteringCoefficients().get(0), 1e-10); // a
        assertEquals(1, result.localClusteringCoefficients().get(1)); // b
        assertEquals(1, result.localClusteringCoefficients().get(2)); // c
        assertEquals(0, result.localClusteringCoefficients().get(3)); // d
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

        LocalClusteringCoefficient.Result result = projectWithoutSeedAndCompute();

        assertEquals(4.0 / 15, result.averageClusteringCoefficient(), 1e-10);
        assertEquals(15, result.localClusteringCoefficients().size());
        assertEquals(1, result.localClusteringCoefficients().get(0)); // a
        assertEquals(1.0 / 3, result.localClusteringCoefficients().get(1)); // b
        assertEquals(1.0 / 3, result.localClusteringCoefficients().get(2)); // c
        assertEquals(1.0 / 3, result.localClusteringCoefficients().get(3)); // d
        assertEquals(1.0 / 3, result.localClusteringCoefficients().get(4)); // e
        assertEquals(1.0 / 3, result.localClusteringCoefficients().get(5)); // f
        assertEquals(1, result.localClusteringCoefficients().get(6)); // g
        assertEquals(1.0 / 3, result.localClusteringCoefficients().get(7)); // h
        assertEquals(0, result.localClusteringCoefficients().get(8)); // i
        assertEquals(0, result.localClusteringCoefficients().get(9)); // j
        assertEquals(0, result.localClusteringCoefficients().get(10)); // k
        assertEquals(0, result.localClusteringCoefficients().get(11)); // l
        assertEquals(0, result.localClusteringCoefficients().get(12)); // m
        assertEquals(0, result.localClusteringCoefficients().get(13)); // n
        assertEquals(0, result.localClusteringCoefficients().get(14)); // o
    }

    private LocalClusteringCoefficient.Result projectWithoutSeedAndCompute() {
        Graph graph = new StoreLoaderBuilder()
            .api(db)
            .globalOrientation(Orientation.UNDIRECTED)
            .build()
            .graph();

        return new LocalClusteringCoefficient(
            graph,
            createConfig().build(),
            AllocationTracker.EMPTY,
            ProgressLogger.NULL_LOGGER
        ).compute();
    }

    private ImmutableLocalClusteringCoefficientBaseConfig.Builder createConfig() {
        return ImmutableLocalClusteringCoefficientBaseConfig.builder();
    }

}

