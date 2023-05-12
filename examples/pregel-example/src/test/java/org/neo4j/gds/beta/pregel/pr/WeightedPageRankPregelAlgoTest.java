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
package org.neo4j.gds.beta.pregel.pr;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.beta.pregel.Pregel;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.paged.HugeDoubleArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.gds.beta.pregel.pr.PageRankPregel.PAGE_RANK;

@GdlExtension
class WeightedPageRankPregelAlgoTest {

    private static final String NODE_CREATE_QUERY =
        "CREATE" +
        "  (a:Label)" +
        ", (b:Label)" +
        ", (c:Label)" +
        ", (d:Label)" +
        ", (e:Label)" +
        ", (f:Label)" +
        ", (g:Label)" +
        ", (h:Label)" +
        ", (i:Label)" +
        ", (j:Label)";


    @GdlGraph(graphNamePrefix = "equalWeight")
    private static final String EQUAL_WEIGHT_GRAPH =
        NODE_CREATE_QUERY +
        "  (b)-[:TYPE2 {weight: 1}]->(c)" +
        ", (c)-[:TYPE2 {weight: 1}]->(b)" +
        ", (d)-[:TYPE2 {weight: 0.5}]->(a)" +
        ", (d)-[:TYPE2 {weight: 0.5}]->(b)" +
        ", (e)-[:TYPE2 {weight: 0.33}]->(b)" +
        ", (e)-[:TYPE2 {weight: 0.33}]->(d)" +
        ", (e)-[:TYPE2 {weight: 0.33}]->(f)" +
        ", (f)-[:TYPE2 {weight: 0.5}]->(b)" +
        ", (f)-[:TYPE2 {weight: 0.5}]->(e)";

    @GdlGraph
    private static final String GRAPH =
        NODE_CREATE_QUERY +
        "  (b)-[:TYPE3 {weight: 1.0}]->(c)" +
        ", (c)-[:TYPE3 {weight: 1.0}]->(b)" +
        ", (d)-[:TYPE3 {weight: 0.3}]->(a)" +
        ", (d)-[:TYPE3 {weight: 0.7}]->(b)" +
        ", (e)-[:TYPE3 {weight: 0.9}]->(b)" +
        ", (e)-[:TYPE3 {weight: 0.05}]->(d)" +
        ", (e)-[:TYPE3 {weight: 0.05}]->(f)" +
        ", (f)-[:TYPE3 {weight: 0.9}]->(b)" +
        ", (f)-[:TYPE3 {weight: 0.1}]->(e)";

    @Inject
    private TestGraph equalWeightGraph;

    @Inject
    private TestGraph graph;

    @Test
    void allWeightsTheSameShouldBeTheSameAsPageRank() {
        var expected = Map.of(
            "a", 0.243007,
            "b", 1.9183995,
            "c", 1.7806315,
            "d", 0.21885,
            "e", 0.243007,
            "f", 0.21885,
            "g", 0.15,
            "h", 0.15,
            "i", 0.15,
            "j", 0.15
        );

        assertResult(equalWeightGraph, expected);
    }

    @Test
    void higherWeightsLeadToHigherPageRank() {
        var expected = Map.of(
            "a", 0.1900095,
            "b", 2.2152279,
            "c", 2.0325884,
            "d", 0.1569275,
            "e", 0.1633280,
            "f", 0.1569275,
            "g", 0.15,
            "h", 0.15,
            "i", 0.15,
            "j", 0.15
        );

        assertResult(graph, expected);
    }

    static void assertResult(TestGraph graph, Map<String, Double> expected) {
        var config = ImmutablePageRankPregelConfig.builder()
            .relationshipWeightProperty("weight")
            .maxIterations(20)
            .isAsynchronous(false)
            .build();

        var pregelJob = Pregel.create(
            graph,
            config,
            new PageRankPregel(),
            Pools.DEFAULT,
            ProgressTracker.NULL_TRACKER
        );

        HugeDoubleArray nodeValues = pregelJob.run().nodeValues().doubleProperties(PAGE_RANK);

        HugeDoubleArray expectedValues = HugeDoubleArray.newArray(graph.nodeCount());

        // normalize expected
        expected.forEach((node, value) -> {
            double total = expected.values().stream().reduce(Double::sum).orElseGet(() -> 0D);
            expectedValues.set(graph.toMappedNodeId(node), expected.get(node) / total);
        });

        // normalize actual values
        double total = nodeValues.stream().sum();
        HugeDoubleArray actualValues = HugeDoubleArray.newArray(graph.nodeCount());
        actualValues.setAll(node -> nodeValues.get(node) / total);

        graph.forEachNode(node -> {
            assertEquals(expectedValues.get(node), actualValues.get(node), 1e-2, "Node#" + node);
            return true;
        });
    }
}
