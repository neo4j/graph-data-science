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
import java.util.stream.LongStream;

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

    @Nested
    @GdlExtension
    class HouseGraph{

        @GdlGraph(orientation = Orientation.UNDIRECTED)
        private static final String DB_CYPHER =
            "CREATE " +
                "  (a0:node)," +
                "  (a1:node)," +
                "  (a2:node)," +
                "  (a3:node)," +
                "  (a4:node)," +
                "(a0)-[:R{w:10}]->(a1)," +
                "(a0)-[:R{w:72}]->(a3)," +
                "(a1)-[:R{w:74}]->(a2)," +
                "(a1)-[:R{w:62}]->(a3)," +
                "(a1)-[:R{w:54}]->(a4)," +
                "(a2)-[:R{w:15}]->(a3)," +
                "(a2)-[:R{w:62}]->(a4)";


        @Inject
        private  TestGraph graph;

        @Test
        void shouldFindCorrectAnswer() {
            LongToDoubleFunction prizes = (x) -> 20.0;

            var pcst =new PCSTFast(graph,prizes,ProgressTracker.NULL_TRACKER);
            var result =pcst.compute();

            var a0 = graph.toMappedNodeId("a0");
            var a1 = graph.toMappedNodeId("a1");

            var parents =result.parentArray();

            boolean case1 =   parents.get(a0) == a1 &&  parents.get(a1) == PrizeSteinerTreeResult.ROOT;
            boolean case2 =   parents.get(a1) == a0 &&  parents.get(a0) == PrizeSteinerTreeResult.ROOT;

            assertThat(
                LongStream
                    .range(0, graph.nodeCount())
                    .filter(v -> v != a0 && v != a1)
                    .map(parents::get)
                    .filter(v -> v != PrizeSteinerTreeResult.PRUNED)
                    .count())
                .isEqualTo(0l);

            assertThat(case1 ^ case2).isTrue();

        }
    }

    @Nested
    @GdlExtension
    class NegativeEdgesGraph{

        @GdlGraph(orientation = Orientation.UNDIRECTED)
        private static final String DB_CYPHER =
            "CREATE " +
                "(a0:Node{prize: 4.0})," +
                "(a1:Node{prize: 4.0})," +
                "(a2:Node{prize: 4.0})," +
                "(a3:Node{prize: 4.0})," +
                "(a0)-[:R{w:-5.0}]->(a1)," +
                "(a0)-[:R{w:-10.0}]->(a3)," +
                "(a1)-[:R{w:-500.0}]->(a3)," +
                "(a1)-[:R{w:-20.0}]->(a2)," +
                "(a2)-[:R{w:-25.0}]->(a3)";
        @Inject
        private  TestGraph graph;

        @Test
        void shouldFindCorrectAnswer() {

            var prizes = graph.nodeProperties("prize");
            var pcst = new PCSTFast(graph, prizes::doubleValue, ProgressTracker.NULL_TRACKER);
            var result = pcst.compute();

            var a0 = graph.toMappedNodeId("a0");
            var a1 = graph.toMappedNodeId("a1");
            var a2 = graph.toMappedNodeId("a2");
            var a3 = graph.toMappedNodeId("a3");

            var parents = result.parentArray();
            assertThat(parents.get(a3)).isEqualTo(a0);
            assertThat(parents.get(a2)).isEqualTo(a1);
            assertThat(parents.get(a1)).isEqualTo(a0);
            assertThat(parents.get(a0)).isEqualTo(PrizeSteinerTreeResult.ROOT);
            var costs = result.relationshipToParentCost();
            assertThat(costs.get(a3)).isEqualTo(-10);
            assertThat(costs.get(a2)).isEqualTo(-20);
            assertThat(costs.get(a1)).isEqualTo(-5);

        }
    }

    @Nested
    @GdlExtension
    class CompletelyDetachedGraphAfterMerging{
        @GdlGraph(orientation = Orientation.UNDIRECTED)
        private static final String DB_CYPHER =
            "CREATE " +
                "(a0:Node{prize: 0.0})," +
                "(a1:Node{prize: 0.0})," +
                "(a2:Node{prize: 0.0})," +
                "(a3:Node{prize: 3.0})," +
                "(a4:Node{prize: 0.0})," +
                "(a5:Node{prize: 3.0})," +
                "(a6:Node{prize: 4.0})," +
                "(a7:Node{prize: 0.0})," +

                "(a6)-[:R{w:-4}]->(a7)," +
                "(a3)-[:R{w:-1}]->(a0)," +
                "(a5)-[:R{w:-2}]->(a1)," +
                "(a4)-[:R{w:-3}]->(a2)";


        @Inject
        private  TestGraph graph;

        @Test
        void shouldWorkProperly(){

            var prizes = graph.nodeProperties("prize");
            var pcst = new PCSTFast(graph, prizes::doubleValue, ProgressTracker.NULL_TRACKER);
            var result = pcst.compute();

            var  a6 =graph.toMappedNodeId("a6");
            var  a7 =graph.toMappedNodeId("a7");


            var parents = result.parentArray();

            for (int i=0;i<6;++i){
                assertThat(parents.get(graph.toMappedNodeId("a"+i))).isEqualTo(PrizeSteinerTreeResult.PRUNED);
            }
            assertThat(parents).satisfiesAnyOf(
                pars-> {
                    assertThat(pars.get(a6)).isEqualTo(a7);
                    assertThat(pars.get(a7)).isEqualTo(PrizeSteinerTreeResult.ROOT);
                },
                pars-> {
                    assertThat(pars.get(a7)).isEqualTo(a6);
                    assertThat(pars.get(a6)).isEqualTo(PrizeSteinerTreeResult.ROOT);
                }
            );
            var costs = result.relationshipToParentCost();
            assertThat(costs).satisfiesAnyOf(
                cost-> {
                    assertThat(cost.get(a6)).isEqualTo(-4);
                },
                cost-> {
                    assertThat(cost.get(a7)).isEqualTo(-4);

                }
            );
        }

    }

}
