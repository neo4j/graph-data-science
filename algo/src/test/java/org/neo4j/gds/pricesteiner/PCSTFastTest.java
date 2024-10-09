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
package org.neo4j.gds.pricesteiner;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.compat.TestLog;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.TaskProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;
import org.neo4j.gds.logging.GdsTestLog;

import java.util.function.LongToDoubleFunction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.assertj.Extractors.removingThreadId;
import static org.neo4j.gds.assertj.Extractors.replaceTimings;

@GdlExtension
class PCSTFastTest {

    @Nested
    @GdlExtension
    class LineGraph {
        @GdlGraph(orientation = Orientation.UNDIRECTED)
        private static final String DB_CYPHER =
            "CREATE " +
                "  (a1:node)," +
                "  (a2:node)," +
                "  (a3:node)," +
                "  (a4:node)," +
                "(a1)-[:R{w:100.0}]->(a2)," +
                "(a2)-[:R{w:10.0}]->(a3)," +
                "(a3)-[:R{w:100.0}]->(a4)";

        @Inject
        private TestGraph graph;

        @Test
        void shouldFindOptimalSolution() {
            LongToDoubleFunction prizes = (x) -> 20.0;

            var pcst = new PCSTFast(graph, prizes, ProgressTracker.NULL_TRACKER);
            var result = pcst.compute();

            var parentArray = result.parentArray();

            assertThat(parentArray.get(graph.toMappedNodeId("a1"))).isEqualTo(PrizeSteinerTreeResult.PRUNED);
            assertThat(parentArray.get(graph.toMappedNodeId("a4"))).isEqualTo(PrizeSteinerTreeResult.PRUNED);

            boolean par1 = (parentArray.get(graph.toMappedNodeId("a2")) == graph.toMappedNodeId("a3")) && (parentArray.get(
                graph.toMappedNodeId("a3")) == PrizeSteinerTreeResult.ROOT);
            boolean par2 = (parentArray.get(graph.toMappedNodeId("a3")) == graph.toMappedNodeId("a2")) && (parentArray.get(
                graph.toMappedNodeId("a2")) == PrizeSteinerTreeResult.ROOT);
            assertThat(par1 ^ par2).isTrue();


        }

        @Test
        void shouldLogProgress() {

            var progressTask = PCSTProgressTrackerTaskCreator.progressTask(graph.nodeCount(),graph.relationshipCount());
            var log = new GdsTestLog();
            var progressTracker = new TaskProgressTracker(progressTask, log, new Concurrency(1), EmptyTaskRegistryFactory.INSTANCE);

           new PCSTFast(graph, x->20, progressTracker).compute();

            Assertions.assertThat(log.getMessages(TestLog.INFO))
                .extracting(removingThreadId())
                .extracting(replaceTimings())
                .containsExactly(
                    "PrizeCollectingSteinerTree :: Start",
                    "PrizeCollectingSteinerTree :: Growth Phase :: Start",
                    "PrizeCollectingSteinerTree :: Growth Phase :: Initialization :: Start",
                    "PrizeCollectingSteinerTree :: Growth Phase :: Initialization 16%",
                    "PrizeCollectingSteinerTree :: Growth Phase :: Initialization 50%",
                    "PrizeCollectingSteinerTree :: Growth Phase :: Initialization 83%",
                    "PrizeCollectingSteinerTree :: Growth Phase :: Initialization 100%",
                    "PrizeCollectingSteinerTree :: Growth Phase :: Initialization :: Finished",
                    "PrizeCollectingSteinerTree :: Growth Phase :: Growing :: Start",
                    "PrizeCollectingSteinerTree :: Growth Phase :: Growing 25%",
                    "PrizeCollectingSteinerTree :: Growth Phase :: Growing 50%",
                    "PrizeCollectingSteinerTree :: Growth Phase :: Growing 75%",
                    "PrizeCollectingSteinerTree :: Growth Phase :: Growing 100%",
                    "PrizeCollectingSteinerTree :: Growth Phase :: Growing :: Finished",
                    "PrizeCollectingSteinerTree :: Growth Phase :: Finished",
                    "PrizeCollectingSteinerTree :: Tree Creation :: Start",
                    "PrizeCollectingSteinerTree :: Tree Creation 25%",
                    "PrizeCollectingSteinerTree :: Tree Creation 100%",
                    "PrizeCollectingSteinerTree :: Tree Creation :: Finished",
                    "PrizeCollectingSteinerTree :: Pruning Phase :: Start",
                    "PrizeCollectingSteinerTree :: Pruning Phase 25%",
                    "PrizeCollectingSteinerTree :: Pruning Phase 50%",
                    "PrizeCollectingSteinerTree :: Pruning Phase 100%",
                    "PrizeCollectingSteinerTree :: Pruning Phase :: Finished",
                    "PrizeCollectingSteinerTree :: Finished"
                );
        }

    }
}
