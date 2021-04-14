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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.beta.pregel.Pregel;
import org.neo4j.graphalgo.beta.pregel.PregelResult;
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

@GdlExtension
class PageRankPregelTest {

    @Nested
    class WikiGraph {

        // https://en.wikipedia.org/wiki/PageRank#/media/File:PageRanks-Example.jpg
        @GdlGraph
        private static final String DB_CYPHER =
            "CREATE" +
            "  (a:Node { expectedRank: 0.3040965 })" +
            ", (b:Node { expectedRank: 3.5658695 })" +
            ", (c:Node { expectedRank: 3.180981 })" +
            ", (d:Node { expectedRank: 0.3625935 })" +
            ", (e:Node { expectedRank: 0.7503465 })" +
            ", (f:Node { expectedRank: 0.3625935 })" +
            ", (g:Node { expectedRank: 0.15 })" +
            ", (h:Node { expectedRank: 0.15 })" +
            ", (i:Node { expectedRank: 0.15 })" +
            ", (j:Node { expectedRank: 0.15 })" +
            ", (k:Node { expectedRank: 0.15 })" +
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
        void withoutTolerance() {
            var config = ImmutablePageRankStreamConfig.builder()
                .maxIterations(40)
                .concurrency(1)
                .tolerance(0)
                .build();

            var gdsResult = runOnGds(graph, config).result().asNodeProperties();
            var pregelResult = runOnPregel(graph, config).nodeValues().doubleProperties(PageRankPregel.PAGE_RANK).asNodeProperties();

            var actual = graph.nodeProperties("expectedRank");

            for (int nodeId = 0; nodeId < graph.nodeCount(); nodeId++) {
                assertThat(gdsResult.doubleValue(nodeId)).isEqualTo(actual.doubleValue(nodeId), within(1e-2));
                assertThat(pregelResult.doubleValue(nodeId)).isEqualTo(actual.doubleValue(nodeId), within(1e-2));
            }
        }

        @ParameterizedTest
        @CsvSource(value = {"0.5, 2", "0.1, 13"})
        void withTolerance(double tolerance, int expectedIterations) {
            var config = ImmutablePageRankStreamConfig.builder()
                .maxIterations(40)
                .concurrency(1)
                .tolerance(tolerance)
                .build();

            var pregelResult = runOnPregel(graph, config);

            // initial iteration is counted extra in Pregel
            assertThat(pregelResult.ranIterations()).isEqualTo(expectedIterations);
        }
    }

    PageRank runOnGds(Graph graph, PageRankBaseConfig config) {
        return PageRankAlgorithmType.NON_WEIGHTED
            .create(graph, config, LongStream.empty(), ProgressLogger.NULL_LOGGER, AllocationTracker.empty())
            .compute();
    }

    PregelResult runOnPregel(Graph graph, PageRankBaseConfig config) {
        var pregelConfig = ImmutablePageRankPregelConfig.builder()
            .maxIterations(config.maxIterations() + 1)
            .dampingFactor(config.dampingFactor())
            .concurrency(config.concurrency())
            .tolerance(config.tolerance())
            .isAsynchronous(false)
            .build();

        var pregelJob = Pregel.create(
            graph,
            pregelConfig,
            new PageRankPregel(pregelConfig),
            Pools.DEFAULT,
            AllocationTracker.empty()
        );

        return pregelJob.run();
    }
}
