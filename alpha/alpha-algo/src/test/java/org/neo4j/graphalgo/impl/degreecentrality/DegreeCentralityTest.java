/*
 * Copyright (c) 2017-2021 "Neo4j,"
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
package org.neo4j.graphalgo.impl.degreecentrality;

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.beta.generator.PropertyProducer;
import org.neo4j.graphalgo.beta.generator.RandomGraphGenerator;
import org.neo4j.graphalgo.beta.generator.RelationshipDistribution;
import org.neo4j.graphalgo.centrality.degreecentrality.DegreeCentrality;
import org.neo4j.graphalgo.config.RandomGraphGeneratorConfig;
import org.neo4j.graphalgo.core.Aggregation;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.huge.HugeGraph;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeDoubleArray;
import org.neo4j.graphalgo.extension.GdlExtension;
import org.neo4j.graphalgo.extension.GdlGraph;
import org.neo4j.graphalgo.extension.IdFunction;
import org.neo4j.graphalgo.extension.Inject;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

@GdlExtension
final class DegreeCentralityTest {

    @GdlGraph(graphNamePrefix = "natural", orientation = Orientation.NATURAL)
    @GdlGraph(graphNamePrefix = "reverse", orientation = Orientation.REVERSE)
    @GdlGraph(graphNamePrefix = "undirected", orientation = Orientation.UNDIRECTED, aggregation = Aggregation.SINGLE)
    private static final String DB_CYPHER =
            "CREATE" +
            "  (a:Label1)" +
            ", (b:Label1)" +
            ", (c:Label1)" +
            ", (d:Label1)" +
            ", (e:Label1)" +
            ", (f:Label1)" +
            ", (g:Label1)" +
            ", (h:Label1)" +
            ", (i:Label1)" +
            ", (j:Label1)" +

            ", (b)-[:TYPE1 {weight: 2.0}]->(c)" +
            ", (c)-[:TYPE1 {weight: 2.0}]->(b)" +

            ", (d)-[:TYPE1 {weight: 2.0}]->(a)" +
            ", (d)-[:TYPE1 {weight: 2.0}]->(b)" +

            ", (e)-[:TYPE1 {weight: 2.0}]->(b)" +
            ", (e)-[:TYPE1 {weight: 2.0}]->(d)" +
            ", (e)-[:TYPE1 {weight: 2.0}]->(f)" +

            ", (f)-[:TYPE1 {weight: 2.0}]->(b)" +
            ", (f)-[:TYPE1 {weight: 2.0}]->(e)";

    @Inject
    private Graph naturalGraph;

    @Inject
    private IdFunction naturalIdFunction;

    @Inject
    private Graph reverseGraph;

    @Inject
    private IdFunction reverseIdFunction;

    @Inject
    private Graph undirectedGraph;

    @Inject
    private IdFunction undirectedIdFunction;

    @Test
    void shouldRunConcurrently() {
        int nodeCount = 20002;
        int averageDegree = 2;
        HugeGraph graph = RandomGraphGenerator
            .builder()
            .nodeCount(nodeCount)
            .averageDegree(averageDegree)
            .relationshipDistribution(RelationshipDistribution.POWER_LAW)
            .seed(0L)
            .relationshipPropertyProducer(PropertyProducer.random("similarity", 0, 1))
            .aggregation(Aggregation.NONE)
            .orientation(Orientation.NATURAL)
            .allowSelfLoops(RandomGraphGeneratorConfig.AllowSelfLoops.NO)
            .allocationTracker(AllocationTracker.empty())
            .build()
            .generate();

        DegreeCentrality degreeCentrality = new DegreeCentrality(
            graph,
            Pools.DEFAULT,
            2,
            true,
            AllocationTracker.empty()
        );

        DegreeCentrality centrality = degreeCentrality.compute();
        HugeDoubleArray centralityResult = centrality.result().array();

        double sum = 0;
        for (double v : centralityResult.toArray()) {
            sum += v;
        }

        double expected = nodeCount * averageDegree * 0.5;
        assertEquals(expected, sum, expected * 0.1);
    }

    @Test
    void outgoingCentrality() {
        final Map<Long, Double> expected = new HashMap<>();

        expected.put(naturalIdFunction.of("a"), 0.0);
        expected.put(naturalIdFunction.of("b"), 1.0);
        expected.put(naturalIdFunction.of("c"), 1.0);
        expected.put(naturalIdFunction.of("d"), 2.0);
        expected.put(naturalIdFunction.of("e"), 3.0);
        expected.put(naturalIdFunction.of("f"), 2.0);
        expected.put(naturalIdFunction.of("g"), 0.0);
        expected.put(naturalIdFunction.of("h"), 0.0);
        expected.put(naturalIdFunction.of("i"), 0.0);
        expected.put(naturalIdFunction.of("j"), 0.0);

        DegreeCentrality degreeCentrality = new DegreeCentrality(
            naturalGraph,
            Pools.DEFAULT,
            4,
            false,
            AllocationTracker.empty()
        );
        degreeCentrality.compute();

        // FIXME: this will fail if there is node ids offset
        IntStream.range(0, expected.size()).forEach(i -> {
            final long nodeId = naturalGraph.toOriginalNodeId(i);
            assertEquals(
                    expected.get(nodeId),
                    degreeCentrality.result().score(i),
                    1e-2,
                    "Node#" + nodeId
            );
        });
    }

    @Test
    void incomingCentrality() {
        final Map<Long, Double> expected = new HashMap<>();

        expected.put(reverseIdFunction.of("b"), 4.0);
        expected.put(reverseIdFunction.of("c"), 1.0);
        expected.put(reverseIdFunction.of("d"), 1.0);
        expected.put(reverseIdFunction.of("e"), 1.0);
        expected.put(reverseIdFunction.of("f"), 1.0);
        expected.put(reverseIdFunction.of("g"), 0.0);
        expected.put(reverseIdFunction.of("h"), 0.0);
        expected.put(reverseIdFunction.of("i"), 0.0);
        expected.put(reverseIdFunction.of("j"), 0.0);
        expected.put(reverseIdFunction.of("a"), 1.0);

        DegreeCentrality degreeCentrality = new DegreeCentrality(
            reverseGraph,
            Pools.DEFAULT,
            4,
            false,
            AllocationTracker.empty()
        );
        degreeCentrality.compute();

        // FIXME: this will fail if there is node ids offset
        IntStream.range(0, expected.size()).forEach(i -> {
            final long nodeId = reverseGraph.toOriginalNodeId(i);
            assertEquals(
                    expected.get(nodeId),
                    degreeCentrality.result().score(i),
                    1e-2,
                    "Node#" + nodeId
            );
        });
    }

    @Test
    void totalCentrality() {
        Map<Long, Double> expected = new HashMap<>();

        // if there are 2 relationships between a pair of nodes these get squashed into a single relationship
        // when we use an undirected graph
        expected.put(undirectedIdFunction.of("a"), 1.0);
        expected.put(undirectedIdFunction.of("b"), 4.0);
        expected.put(undirectedIdFunction.of("c"), 1.0);
        expected.put(undirectedIdFunction.of("d"), 3.0);
        expected.put(undirectedIdFunction.of("e"), 3.0);
        expected.put(undirectedIdFunction.of("f"), 2.0);
        expected.put(undirectedIdFunction.of("g"), 0.0);
        expected.put(undirectedIdFunction.of("h"), 0.0);
        expected.put(undirectedIdFunction.of("i"), 0.0);
        expected.put(undirectedIdFunction.of("j"), 0.0);

        DegreeCentrality degreeCentrality = new DegreeCentrality(
            undirectedGraph,
            Pools.DEFAULT,
            4,
            false,
            AllocationTracker.empty()
        );
        degreeCentrality.compute();

        // FIXME: this will fail if there is node ids offset
        IntStream.range(0, expected.size()).forEach(i -> {
            long nodeId = undirectedGraph.toOriginalNodeId(i);
            assertEquals(
                    expected.get(nodeId),
                    degreeCentrality.result().score(i),
                    1e-2,
                    "Node#" + nodeId + "[" + i + "]"
            );
        });
    }
}
