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
package org.neo4j.graphalgo.pagerank;

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.nodeproperties.DoubleNodeProperties;
import org.neo4j.graphalgo.beta.pregel.Pregel;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.extension.GdlExtension;
import org.neo4j.graphalgo.extension.GdlGraph;
import org.neo4j.graphalgo.extension.Inject;
import org.neo4j.graphalgo.extension.TestGraph;

import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;
import static org.neo4j.graphalgo.pagerank.PageRankPregel.PAGE_RANK;

@GdlExtension
class PageRankWikiTest {

    @GdlGraph
    private static final String DB_CYPHER =
        "CREATE" +
        "  (a:Node)" +
        ", (b:Node)" +
        ", (c:Node)" +
        ", (d:Node)" +
        ", (e:Node)" +
        ", (f:Node)" +
        ", (g:Node)" +
        ", (h:Node)" +
        ", (i:Node)" +
        ", (j:Node)" +
        ", (k:Node)" +
        ", (b)-[:TYPE]->(c)" +
        ", (c)-[:TYPE]->(b)" +
        ", (d)-[:TYPE]->(a)" +
        ", (d)-[:TYPE]->(b)" +
        ", (e)-[:TYPE]->(b)" +
        ", (e)-[:TYPE]->(d)" +
        ", (e)-[:TYPE]->(f)" +
        ", (f)-[:TYPE]->(b)" +
        ", (f)-[:TYPE]->(e)" +
        ", (g)-[:TYPE]->(b)" +
        ", (g)-[:TYPE]->(e)" +
        ", (h)-[:TYPE]->(b)" +
        ", (h)-[:TYPE]->(e)" +
        ", (i)-[:TYPE]->(b)" +
        ", (i)-[:TYPE]->(e)" +
        ", (j)-[:TYPE]->(e)" +
        ", (k)-[:TYPE]->(e)";

    @Inject
    private TestGraph graph;

    @Test
    void test() {
        var expected = new double[Math.toIntExact(graph.nodeCount())];
        expected[(int) graph.toMappedNodeId("a")] = 0.3040965;
        expected[(int) graph.toMappedNodeId("b")] = 3.5658695;
        expected[(int) graph.toMappedNodeId("c")] = 3.180981;
        expected[(int) graph.toMappedNodeId("d")] = 0.3625935;
        expected[(int) graph.toMappedNodeId("e")] = 0.7503465;
        expected[(int) graph.toMappedNodeId("f")] = 0.3625935;
        expected[(int) graph.toMappedNodeId("g")] = 0.15;
        expected[(int) graph.toMappedNodeId("h")] = 0.15;
        expected[(int) graph.toMappedNodeId("i")] = 0.15;
        expected[(int) graph.toMappedNodeId("j")] = 0.15;
        expected[(int) graph.toMappedNodeId("k")] = 0.15;

        var config = ImmutablePageRankStreamConfig.builder()
            .maxIterations(40)
            .concurrency(1)
            .tolerance(0)
            .build();

        var gdsResult = runOnGds(graph, config);
        var pregelResult = runOnPregel(graph, config);

        LongStream.range(0, graph.nodeCount())
            .forEach(nodeId -> System.out.println("gds:" + gdsResult.doubleValue(nodeId) + " pregel:" + pregelResult.doubleValue(
                nodeId)));

        for (int nodeId = 0; nodeId < graph.nodeCount(); nodeId++) {
            assertThat(gdsResult.doubleValue(nodeId)).isEqualTo(expected[nodeId], within(1e-2));
            assertThat(pregelResult.doubleValue(nodeId)).isEqualTo(expected[nodeId], within(1e-2));
        }
    }

    DoubleNodeProperties runOnGds(Graph graph, PageRankBaseConfig config) {
        return PageRankAlgorithmType.NON_WEIGHTED
            .create(graph, config, LongStream.empty(), ProgressLogger.NULL_LOGGER, AllocationTracker.empty())
            .compute()
            .result()
            .asNodeProperties();
    }

    DoubleNodeProperties runOnPregel(Graph graph, PageRankBaseConfig config) {
        var pregelConfig = ImmutablePageRankPregelConfig.builder()
            .maxIterations(config.maxIterations() + 1)
            .dampingFactor(config.dampingFactor())
            .concurrency(config.concurrency())
            .isAsynchronous(false)
            .build();

        var pregelJob = Pregel.create(
            graph,
            pregelConfig,
            new PageRankPregel(pregelConfig),
            Pools.DEFAULT,
            AllocationTracker.empty()
        );

        return pregelJob.run().nodeValues().doubleProperties(PAGE_RANK).asNodeProperties();
    }
}
