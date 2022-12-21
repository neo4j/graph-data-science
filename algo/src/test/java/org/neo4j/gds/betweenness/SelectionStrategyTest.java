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
package org.neo4j.gds.betweenness;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.beta.generator.RandomGraphGenerator;
import org.neo4j.gds.beta.generator.RelationshipDistribution;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.gds.TestSupport.fromGdl;

@GdlExtension
class SelectionStrategyTest {

    @GdlGraph
    private static final String DB_GDL =
        "(a)-->(b)" +
        "(a)-->(c)" +
        "(a)-->(d)" +
        "(a)-->(e)" +
        "(a)-->(f)" +
        "(a)-->(g)" +

        "(b)-->(h)" +
        "(b)-->(i)" +
        "(b)-->(j)" +
        "(b)-->(k)";

    @Inject
    private TestGraph graph;

    @Test
    void selectAll() {
        SelectionStrategy selectionStrategy = new FullSelectionStrategy();
        selectionStrategy.init(graph, Pools.DEFAULT, 1);
        assertEquals(graph.nodeCount(), samplingSize(selectionStrategy));
    }

    @ParameterizedTest
    @ValueSource(longs = {0, 1, 2, 10, 11})
    void selectSamplingSize(long samplingSize) {
        SelectionStrategy selectionStrategy = new RandomDegreeSelectionStrategy(samplingSize);
        selectionStrategy.init(graph, Pools.DEFAULT, 1);
        assertEquals(samplingSize, samplingSize(selectionStrategy));
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 4, 42, 1337, 99_999, 100_000})
    void selectSamplingSizeMultiThreaded(long samplingSize) {
        var graph = RandomGraphGenerator.builder()
            .nodeCount(100_000)
            .averageDegree(10)
            .relationshipDistribution(RelationshipDistribution.RANDOM)
            .build()
            .generate();
        SelectionStrategy selectionStrategy = new RandomDegreeSelectionStrategy(samplingSize, Optional.of(42L));
        selectionStrategy.init(graph, Pools.DEFAULT, 4);
        assertEquals(samplingSize, samplingSize(selectionStrategy));
    }

    @Test
    void selectSamplingSizeWithSeed() {
        SelectionStrategy selectionStrategy = new RandomDegreeSelectionStrategy(3, Optional.of(42L));
        selectionStrategy.init(graph, Pools.DEFAULT, 1);
        assertEquals(3, samplingSize(selectionStrategy));
        selectionStrategy.init(graph, Pools.DEFAULT, 1);
        assertEquals(graph.toMappedNodeId("a"), selectionStrategy.next());
        assertEquals(graph.toMappedNodeId("b"), selectionStrategy.next());
        assertEquals(graph.toMappedNodeId("f"), selectionStrategy.next());
    }

    @Test
    void neverIncludeZeroDegNodesIfBetterChoicesExist() {
        TestGraph graph = fromGdl("(), (), (), (), (), (a)-->(), (), (), ()");

        SelectionStrategy selectionStrategy = new RandomDegreeSelectionStrategy(1);
        selectionStrategy.init(graph, Pools.DEFAULT, 1);
        assertEquals(1, samplingSize(selectionStrategy));
        selectionStrategy.init(graph, Pools.DEFAULT, 1);
        assertEquals(graph.toMappedNodeId("a"), selectionStrategy.next());
    }

    @Test
    void not100PercentLikelyUnlessMaxDegNode() {
        TestGraph graph = fromGdl("(a)-->(b), (b)-->(c), (b)-->(d)");

        SelectionStrategy selectionStrategy = new RandomDegreeSelectionStrategy(1, Optional.of(42L));
        selectionStrategy.init(graph, Pools.DEFAULT, 1);
        assertEquals(1, samplingSize(selectionStrategy));
        selectionStrategy.init(graph, Pools.DEFAULT, 1);
        assertEquals(graph.toMappedNodeId("b"), selectionStrategy.next());
    }

    @Test
    void selectHighDegreeNode() {
        SelectionStrategy selectionStrategy = new RandomDegreeSelectionStrategy(1);
        selectionStrategy.init(graph, Pools.DEFAULT, 1);
        assertEquals(1, samplingSize(selectionStrategy));
        selectionStrategy.init(graph, Pools.DEFAULT, 1);
        var isA = selectionStrategy.next();
        var isB = selectionStrategy.next();
        assertTrue(isA != SelectionStrategy.NONE_SELECTED || isB != SelectionStrategy.NONE_SELECTED);
    }

    private static long samplingSize(SelectionStrategy selectionStrategy) {
        long samplingSize = 0;
        while (selectionStrategy.next() != SelectionStrategy.NONE_SELECTED) {
            samplingSize++;
        }
        return samplingSize;
    }
}
