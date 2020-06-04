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
package org.neo4j.graphalgo.impl.betweenness;

import org.junit.Assert;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.beta.generator.RandomGraphGenerator;
import org.neo4j.graphalgo.beta.generator.RelationshipDistribution;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.gdl.GdlFactory;

import java.util.Locale;
import java.util.Optional;
import java.util.Random;
import java.util.stream.Stream;

class BetweennessCentralityComparisonTest {

    static Stream<Arguments> randomGraphParameters() {
        var rand = new Random();
        return Stream.of(
            Arguments.of(10, 2, RelationshipDistribution.UNIFORM, rand.nextLong(), 1),
            Arguments.of(10, 2, RelationshipDistribution.RANDOM, rand.nextLong(), 1),
            Arguments.of(10, 2, RelationshipDistribution.POWER_LAW, rand.nextLong(), 1),
            Arguments.of(100, 4, RelationshipDistribution.UNIFORM, rand.nextLong(), 1),
            Arguments.of(100, 4, RelationshipDistribution.RANDOM, rand.nextLong(), 1),
            Arguments.of(100, 4, RelationshipDistribution.POWER_LAW, rand.nextLong(), 1),
            Arguments.of(1_000, 10, RelationshipDistribution.UNIFORM, rand.nextLong(), 1),
            Arguments.of(1_000, 10, RelationshipDistribution.RANDOM, rand.nextLong(), 1),
            Arguments.of(1_000, 10, RelationshipDistribution.POWER_LAW, rand.nextLong(), 1),
            Arguments.of(10, 2, RelationshipDistribution.UNIFORM, rand.nextLong(), 4),
            Arguments.of(10, 2, RelationshipDistribution.RANDOM, rand.nextLong(), 4),
            Arguments.of(10, 2, RelationshipDistribution.POWER_LAW, rand.nextLong(), 4),
            Arguments.of(100, 4, RelationshipDistribution.UNIFORM, rand.nextLong(), 4),
            Arguments.of(100, 4, RelationshipDistribution.RANDOM, rand.nextLong(), 4),
            Arguments.of(100, 4, RelationshipDistribution.POWER_LAW, rand.nextLong(), 4),
            Arguments.of(1_000, 10, RelationshipDistribution.UNIFORM, rand.nextLong(), 4),
            Arguments.of(1_000, 10, RelationshipDistribution.RANDOM, rand.nextLong(), 4),
            Arguments.of(1_000, 10, RelationshipDistribution.POWER_LAW, rand.nextLong(), 4)
        );
    }

    @ParameterizedTest
    @MethodSource("randomGraphParameters")
    void shouldHaveSameResultOnRandomGraph(
        long nodeCount,
        long averageDegree,
        RelationshipDistribution distribution,
        long seed,
        int concurreny
    ) {
        var graph = new RandomGraphGenerator(
            nodeCount,
            averageDegree,
            distribution,
            seed,
            Optional.empty(),
            AllocationTracker.EMPTY
        ).generate();

        compareResults(graph, concurreny);
    }

    static Stream<Graph> specialGraphs() {
        return Stream.of(
            // [0.0, 3.0, 0.0, 2.0, 1.0]
            "CREATE " +
            "  (n0)" +
            ", (n1)" +
            ", (n2)" +
            ", (n3)" +
            ", (n4)" +
            ",  (n0)-[:REL]->(n1)" +
            ",  (n1)-[:REL]->(n2)" +
            ",  (n1)-[:REL]->(n3)" +
            ",  (n3)-[:REL]->(n4)" +
            ",  (n4)-[:REL]->(n2)",
            // [0.0, 2.0, 0.0, 0.0, 0.0]
            "CREATE" +
            "  (n0)" +
            ", (n1)" +
            ", (n2)" +
            ", (n3)" +
            ", (n4)" +
            ", (n0)-[:REL]->(n2)" +
            ", (n1)-[:REL]->(n0)" +
            ", (n1)-[:REL]->(n2)" +
            ", (n4)-[:REL]->(n1)",
            // [0.6666666666666666, 1.8333333333333333, 0.0, 0.0, 0.5]
            "CREATE " +
            "  (n0)" +
            ", (n1)" +
            ", (n2)" +
            ", (n3)" +
            ", (n1)-[:REL]->(n0)" +
            ", (n2)-[:REL]->(n1)" +
            ", (n2)-[:REL]->(n3)" +
            ", (n3)-[:REL]->(n0)"
        ).map(cypher -> GdlFactory.of(cypher).build().graphStore().getUnion());
    }

    @ParameterizedTest
    @MethodSource("specialGraphs")
    void shouldProduceSameResult(Graph graph) {
        compareResults(graph, 1);
    }

    private void compareResults(Graph graph, int concurrency) {
        var msBc = new MSBetweennessCentrality(graph, false, 1, Pools.DEFAULT, concurrency, AllocationTracker.EMPTY);
        msBc.compute();

        var bc = new BetweennessCentrality(graph, Pools.DEFAULT, concurrency, false);
        bc.compute();

        for (int i = 0; i < graph.nodeCount(); i++) {
            Assert.assertEquals(
                String.format(Locale.ENGLISH, "node %d with wrong BC value", i),
                bc.getCentrality().get(i),
                msBc.getCentrality().get(i),
                1E-3
            );
        }
    }
}
