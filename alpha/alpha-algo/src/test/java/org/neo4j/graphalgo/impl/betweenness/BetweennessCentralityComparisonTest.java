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
import org.neo4j.graphalgo.AlgoTestBase;
import org.neo4j.graphalgo.StoreLoaderBuilder;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.beta.generator.RandomGraphGenerator;
import org.neo4j.graphalgo.beta.generator.RelationshipDistribution;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;

import java.util.Locale;
import java.util.Optional;
import java.util.stream.Stream;

class BetweennessCentralityComparisonTest extends AlgoTestBase {

    static Stream<Arguments> randomGraphParameters() {
        return Stream.of(
            Arguments.of(100, 4, RelationshipDistribution.UNIFORM, null),
            Arguments.of(100, 4, RelationshipDistribution.RANDOM, null),
            Arguments.of(100, 4, RelationshipDistribution.POWER_LAW, null)
        );
    }

    @ParameterizedTest
    @MethodSource("randomGraphParameters")
    void shouldHaveSameResultOnRandomGraph(long nodeCount, long averageDegree, RelationshipDistribution distribution, Long seed) {
        var graph = new RandomGraphGenerator(
            nodeCount,
            averageDegree,
            distribution,
            seed,
            Optional.empty(),
            AllocationTracker.EMPTY
        ).generate();

        compareResults(graph);
    }

    static Stream<String> specialGraphs() {
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
        );
    }

    @ParameterizedTest
    @MethodSource("specialGraphs")
    void shouldProduceSameResult(String graphQuery) {
        runQuery("MATCH (n) DETACH DELETE n");
        runQuery(graphQuery);
        compareResults(new StoreLoaderBuilder().api(db).build().graph());
    }

    private void compareResults(Graph graph) {
        var msBc = new MSBetweennessCentrality(graph, false, 1, Pools.DEFAULT, 1, AllocationTracker.EMPTY);
        msBc.compute();

        var bc = new BetweennessCentrality(graph, Pools.DEFAULT, 1, false);
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
