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
package org.neo4j.gds.steiner;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.TestProgressTracker;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.compat.TestLog;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.assertj.Extractors.removingThreadId;
import static org.neo4j.gds.assertj.Extractors.replaceTimings;

@GdlExtension
class ShortestPathsSteinerAlgorithmReroutingTest {

    static String getTwoReroutesQuery = "CREATE " +
                                        "  (a0:Node)," +
                                        "  (a1:Node)," +
                                        "  (a2:Node)," +
                                        "  (a3:Node)," +
                                        "  (a4:Node)," +
                                        "  (a5:Node)," +
                                        "  (a6:Node)," +
                                        "  (a7:Node)," +


                                        "  (a0)-[:R {weight: 1.0}]->(a1)," +
                                        "  (a0)-[:R {weight: 10.0}]->(a4)," +
                                        "  (a0)-[:R {weight: 10.0}]->(a7)," +

                                        "  (a1)-[:R {weight: 1.0}]->(a2)," +
                                        "  (a2)-[:R {weight: 1.0}]->(a3)," +

                                        "  (a4)-[:R {weight: 1.0}]->(a3)," +

                                        "  (a3)-[:R {weight: 1.0}]->(a5)," +
                                        "  (a5)-[:R {weight: 1.0}]->(a6)," +

                                        "  (a4)-[:R {weight: 1.0}]->(a6) ";

    static String getGraphQuery = "CREATE " +
                                  "  (a0:Node)," +
                                  "  (a1:Node)," +
                                  "  (a2:Node)," +
                                  "  (a3:Node)," +
                                  "  (a4:Node)," +
                                  " (a5:Node)," +

                                  "  (a0)-[:R {weight: 1.0}]->(a1)," +
                                  "  (a0)-[:R {weight: 4.0}]->(a4)," +

                                  "  (a1)-[:R {weight: 1.0}]->(a2)," +
                                  "  (a2)-[:R {weight: 1.0}]->(a3)," +

                                  "  (a4)-[:R {weight: 0.0}]->(a3)";

    static String getNoRerouteQuery = "CREATE " +
                                      "  (a0:Node)," +
                                      "  (a1:Node)," +
                                      "  (a2:Node)," +
                                      "  (a3:Node)," +
                                      "  (a0)-[:R {weight: 1.0}]->(a1)," +
                                      "  (a1)-[:R {weight: 1.0}]->(a2)," +
                                      "  (a2)-[:R {weight: 1.0}]->(a3)," +
                                      "  (a3)-[:R {weight: 0.5}]->(a1),";

    static String getCrossRoadQuery = "CREATE " +
                                      "  (a0:Node)," +
                                      "  (a1:Node)," +

                                      "  (a2:Node)," +
                                      "  (a3:Node)," +

                                      "  (a4:Node)," +
                                      "  (a5:Node)," +

                                      "  (a6:Node)," +

                                      "  (a0)-[:R {weight: 10.0}]->(a1)," +
                                      "  (a0)-[:R {weight: 100.0}]->(a6)," +

                                      "  (a1)-[:R {weight: 10.0}]->(a2)," +
                                      "  (a1)-[:R {weight: 20.0}]->(a4)," +

                                      "  (a2)-[:R {weight: 10.0}]->(a3)," +

                                      "  (a4)-[:R {weight: 20.0}]->(a5)," +

                                      "  (a6)-[:R {weight: 1.0}]->(a3)";

    static String getDocsExampleQuery = "CREATE (a0:Node)," +
                                        "(a1: Node)" +
                                        "(a2: Node)" +
                                        "(a3: Node)" +
                                        "(a4: Node)" +
                                        "(a5: Node)" +
                                        "(a0)-[:R {weight:10}]->(a5)," +
                                        "(a0)-[:R {weight:1}]->(a1)," +
                                        "(a0)-[:R {weight:7}]->(a4)," +
                                        "(a1)-[:R {weight:1}]->(a2)," +
                                        "(a2)-[:R {weight:4}]->(a3)," +
                                        "(a2)-[:R {weight:6}]->(a4)," +
                                        "(a5)-[:R {weight:3}]->(a3)";

    @GdlGraph(orientation = Orientation.NATURAL)
    private static final String DB_CYPHER = getGraphQuery;
    @Inject
    private TestGraph graph;

    @GdlGraph(orientation = Orientation.NATURAL, graphNamePrefix = "inv", indexInverse = true)
    private static final String invDB_CYPHER = getGraphQuery;
    @Inject
    private TestGraph invGraph;
    //////////////
    @GdlGraph(graphNamePrefix = "noReroute")
    private static final String nodeCreateQuery = getNoRerouteQuery;
    @Inject
    private TestGraph noRerouteGraph;

    @GdlGraph(graphNamePrefix = "invnoReroute", indexInverse = true)
    private static final String invnodeCreateQuery = getNoRerouteQuery;
    @Inject
    private TestGraph invnoRerouteGraph;
    ///////////
    @GdlGraph(graphNamePrefix = "twoReroutes")
    private static final String twoReroutesQuery = getTwoReroutesQuery;
    @Inject
    private TestGraph twoReroutesGraph;

    @GdlGraph(graphNamePrefix = "invtwoReroutes", indexInverse = true)
    private static final String invtwoReroutesQuery = getTwoReroutesQuery;
    @Inject
    private TestGraph invtwoReroutesGraph;
    ////////

    @GdlGraph(graphNamePrefix = "crossRoad")
    private static final String crossRoadQuery = getCrossRoadQuery;
    @Inject
    private TestGraph crossRoadGraph;
    @GdlGraph(graphNamePrefix = "invcrossRoad", indexInverse = true)
    private static final String invcrossRoadQuery = getCrossRoadQuery;
    @Inject
    private TestGraph invcrossRoadGraph;

    @GdlGraph(graphNamePrefix = "docs", indexInverse = true)
    private static final String docs = getDocsExampleQuery;
    @Inject
    private TestGraph docsGraph;

    @Test
    void shouldPruneUnusedIfRerouting() {
        IdFunction idFunction = graph::toMappedNodeId;

        var steinerResult = new ShortestPathsSteinerAlgorithm(
            graph,
            idFunction.of("a0"),
            List.of(idFunction.of("a3"), idFunction.of("a4")),
            2.0,
            1,
            false,
            Pools.DEFAULT,
            ProgressTracker.NULL_TRACKER
        ).compute();
        assertThat(steinerResult.totalCost()).isEqualTo(7.0);
        assertThat(steinerResult.effectiveNodeCount()).isEqualTo(5);
        assertThat(steinerResult.effectiveTargetNodesCount()).isEqualTo(2);

        var steinerResultWithReroute = new ShortestPathsSteinerAlgorithm(
            graph,
            idFunction.of("a0"),
            List.of(idFunction.of("a3"), idFunction.of("a4")),
            2.0,
            1,
            true,
            Pools.DEFAULT,
            ProgressTracker.NULL_TRACKER
        ).compute();
        assertThat(steinerResultWithReroute.totalCost()).isEqualTo(4.0);
        assertThat(steinerResultWithReroute.effectiveNodeCount()).isEqualTo(3);
        assertThat(steinerResultWithReroute.effectiveTargetNodesCount()).isEqualTo(2);

    }

    @Test
    void shouldPruneUnusedIfReroutingOnInvertedIndex() {
        IdFunction invIdFunction = invGraph::toMappedNodeId;

        var steinerResultWithReroute = new ShortestPathsSteinerAlgorithm(
            invGraph,
            invIdFunction.of("a0"),
            List.of(invIdFunction.of("a3"), invIdFunction.of("a4")),
            2.0,
            1,
            true,
            Pools.DEFAULT,
            ProgressTracker.NULL_TRACKER
        ).compute();
        assertThat(steinerResultWithReroute.totalCost()).isEqualTo(4.0);
        assertThat(steinerResultWithReroute.effectiveNodeCount()).isEqualTo(3);
        assertThat(steinerResultWithReroute.effectiveTargetNodesCount()).isEqualTo(2);
    }

    @Test
    void rerouteShouldNotCreateLoops() {
        IdFunction noRerouteIdFunction = noRerouteGraph::toMappedNodeId;

        var steinerResult = new ShortestPathsSteinerAlgorithm(
            noRerouteGraph,
            noRerouteIdFunction.of("a0"),
            List.of(noRerouteIdFunction.of("a3")),
            2.0,
            1,
            true,
            Pools.DEFAULT,
            ProgressTracker.NULL_TRACKER
        ).compute();
        var parent = steinerResult.parentArray().toArray();

        assertThat(parent[(int) noRerouteIdFunction.of("a0")]).isEqualTo(ShortestPathsSteinerAlgorithm.ROOT_NODE);
        assertThat(parent[(int) noRerouteIdFunction.of("a1")]).isEqualTo(noRerouteIdFunction.of("a0"));
        assertThat(parent[(int) noRerouteIdFunction.of("a2")]).isEqualTo(noRerouteIdFunction.of("a1"));
        assertThat(parent[(int) noRerouteIdFunction.of("a3")]).isEqualTo(noRerouteIdFunction.of("a2"));

        assertThat(steinerResult.totalCost()).isEqualTo(3);

    }

    @Test
    void rerouteShouldNotCreateLoopsOnInvertedIndex() {
        IdFunction invnoRerouteIdFunction = invnoRerouteGraph::toMappedNodeId;

        var steinerResult = new ShortestPathsSteinerAlgorithm(
            invnoRerouteGraph,
            invnoRerouteIdFunction.of("a0"),
            List.of(invnoRerouteIdFunction.of("a3")),
            2.0,
            1,
            true,
            Pools.DEFAULT,
            ProgressTracker.NULL_TRACKER
        ).compute();
        var parent = steinerResult.parentArray().toArray();

        assertThat(parent[(int) invnoRerouteIdFunction.of("a0")]).isEqualTo(ShortestPathsSteinerAlgorithm.ROOT_NODE);
        assertThat(parent[(int) invnoRerouteIdFunction.of("a1")]).isEqualTo(invnoRerouteIdFunction.of("a0"));
        assertThat(parent[(int) invnoRerouteIdFunction.of("a2")]).isEqualTo(invnoRerouteIdFunction.of("a1"));
        assertThat(parent[(int) invnoRerouteIdFunction.of("a3")]).isEqualTo(invnoRerouteIdFunction.of("a2"));

        assertThat(steinerResult.totalCost()).isEqualTo(3);

    }

    @Test
    void shouldWorkForUnreachableAndReachableTerminals() {
        IdFunction idFunction = graph::toMappedNodeId;

        Assertions.assertDoesNotThrow(() -> {

            var steinerTreeResult = new ShortestPathsSteinerAlgorithm(
                graph,
                idFunction.of("a0"),
                List.of(idFunction.of("a3"), idFunction.of("a4"), idFunction.of("a5")),
                2.0,
                1,
                true,
                Pools.DEFAULT,
                ProgressTracker.NULL_TRACKER
            ).compute();
            assertThat(steinerTreeResult.effectiveTargetNodesCount()).isEqualTo(2);
        });

    }

    @Test
    void shouldWorkIfNoReachableTerminals() {
        IdFunction idFunction = graph::toMappedNodeId;

        Assertions.assertDoesNotThrow(() -> {

            var steinerTreeResult = new ShortestPathsSteinerAlgorithm(
                graph,
                idFunction.of("a0"),
                List.of(idFunction.of("a5")),
                2.0,
                1,
                true,
                Pools.DEFAULT,
                ProgressTracker.NULL_TRACKER
            ).compute();
            assertThat(steinerTreeResult.effectiveTargetNodesCount()).isEqualTo(0);
            assertThat(steinerTreeResult.effectiveNodeCount()).isEqualTo(1);

        });
    }

    @Test
    void shouldLogProgress() {
        var sourceId = graph.toOriginalNodeId("a0");
        var target1 = graph.toOriginalNodeId("a3");
        var target2 = graph.toOriginalNodeId("a4");

        var config = SteinerTreeStatsConfigImpl
            .builder()
            .sourceNode(sourceId)
            .targetNodes(List.of(target1, target2))
            .build();

        var steinerTreeAlgorithmFactory = new SteinerTreeAlgorithmFactory();
        var log = Neo4jProxy.testLog();
        Task baseTask = steinerTreeAlgorithmFactory.progressTask(graph, config);
        var progressTracker = new TestProgressTracker(
            baseTask,
            log,
            4,
            EmptyTaskRegistryFactory.INSTANCE
        );


        steinerTreeAlgorithmFactory.build(graph, config, progressTracker).compute();

        assertThat(log.getMessages(TestLog.INFO))
            .extracting(removingThreadId())
            .extracting(replaceTimings())
            .containsExactly(
                "SteinerTree :: Start",
                "SteinerTree :: Traverse :: Start",
                "SteinerTree :: Traverse 50%",
                "SteinerTree :: Traverse 100%",
                "SteinerTree :: Traverse :: Finished",
                "SteinerTree :: Finished"
            );
    }


    @Test
    void shouldLogProgressWithRerouting() {

        var sourceId = graph.toOriginalNodeId("a0");
        var target1 = graph.toOriginalNodeId("a3");
        var target2 = graph.toOriginalNodeId("a4");

        var config = SteinerTreeStatsConfigImpl
            .builder()
            .sourceNode(sourceId)
            .applyRerouting(true)
            .targetNodes(List.of(target1, target2))
            .build();

        var steinerTreeAlgorithmFactory = new SteinerTreeAlgorithmFactory();
        var log = Neo4jProxy.testLog();
        Task baseTask = steinerTreeAlgorithmFactory.progressTask(graph, config);
        var progressTracker = new TestProgressTracker(
            baseTask,
            log,
            4,
            EmptyTaskRegistryFactory.INSTANCE
        );

        steinerTreeAlgorithmFactory.build(graph, config, progressTracker).compute();

        assertThat(log.getMessages(TestLog.INFO))
            .extracting(removingThreadId())
            .extracting(replaceTimings())
            .containsExactly(
                "SteinerTree :: Start",
                "SteinerTree :: Traverse :: Start",
                "SteinerTree :: Traverse 50%",
                "SteinerTree :: Traverse 100%",
                "SteinerTree :: Traverse :: Finished",
                "SteinerTree :: Reroute :: Start",
                "SteinerTree :: Reroute 16%",
                "SteinerTree :: Reroute 33%",
                "SteinerTree :: Reroute 50%",
                "SteinerTree :: Reroute 66%",
                "SteinerTree :: Reroute 83%",
                "SteinerTree :: Reroute 100%",
                "SteinerTree :: Reroute :: Finished",
                "SteinerTree :: Finished"
            );
    }

    @Test
    void shouldLogProgressWithInverseRerouting() {

        var sourceId = invGraph.toOriginalNodeId("a0");
        var target1 = invGraph.toOriginalNodeId("a3");
        var target2 = invGraph.toOriginalNodeId("a4");

        var config = SteinerTreeStatsConfigImpl
            .builder()
            .sourceNode(sourceId)
            .applyRerouting(true)
            .targetNodes(List.of(target1, target2))
            .build();

        var steinerTreeAlgorithmFactory = new SteinerTreeAlgorithmFactory();
        var log = Neo4jProxy.testLog();
        Task baseTask = steinerTreeAlgorithmFactory.progressTask(invGraph, config);
        var progressTracker = new TestProgressTracker(
            baseTask,
            log,
            4,
            EmptyTaskRegistryFactory.INSTANCE
        );

        steinerTreeAlgorithmFactory.build(invGraph, config, progressTracker).compute();

        assertThat(log.getMessages(TestLog.INFO))
            .extracting(removingThreadId())
            .extracting(replaceTimings())
            .containsExactly(
                "SteinerTree :: Start",
                "SteinerTree :: Traverse :: Start",
                "SteinerTree :: Traverse 50%",
                "SteinerTree :: Traverse 100%",
                "SteinerTree :: Traverse :: Finished",
                "SteinerTree :: Reroute :: Start",
                "SteinerTree :: Reroute 16%",
                "SteinerTree :: Reroute 33%",
                "SteinerTree :: Reroute 50%",
                "SteinerTree :: Reroute 66%",
                "SteinerTree :: Reroute 100%",
                "SteinerTree :: Reroute :: Finished",
                "SteinerTree :: Finished"
            );
    }


    @Test
    void shouldNotGetOptimalWithoutBetterRerouting() {
        IdFunction twoReroutesIdFunction = twoReroutesGraph::toMappedNodeId;

        var steinerResultWithReroute = new ShortestPathsSteinerAlgorithm(
            twoReroutesGraph,
            twoReroutesIdFunction.of("a0"),
            List.of(
                twoReroutesIdFunction.of("a3"),
                twoReroutesIdFunction.of("a4"),
                twoReroutesIdFunction.of("a6"),
                twoReroutesIdFunction.of("a7")
            ),
            2.0,
            1,
            true,
            Pools.DEFAULT,
            ProgressTracker.NULL_TRACKER
        ).compute();
        assertThat(steinerResultWithReroute.totalCost()).isEqualTo(25.0);
        assertThat(steinerResultWithReroute.effectiveNodeCount()).isEqualTo(8);
        assertThat(steinerResultWithReroute.effectiveTargetNodesCount()).isEqualTo(4);

    }

    @Test
    void shouldHandleMultiplePruningsOnSameTreeAndGetBetter() {
        IdFunction invtwoReroutesIdFunction = invtwoReroutesGraph::toMappedNodeId;

        var steinerResultWithReroute = new ShortestPathsSteinerAlgorithm(
            invtwoReroutesGraph,
            invtwoReroutesIdFunction.of("a0"),
            List.of(
                invtwoReroutesIdFunction.of("a3"),
                invtwoReroutesIdFunction.of("a4"),
                invtwoReroutesIdFunction.of("a6"),
                invtwoReroutesIdFunction.of("a7")
            ),
            2.0,
            1,
            true,
            Pools.DEFAULT,
            ProgressTracker.NULL_TRACKER
        ).compute();
        assertThat(steinerResultWithReroute.totalCost()).isEqualTo(22.0);
        assertThat(steinerResultWithReroute.effectiveNodeCount()).isEqualTo(5);
        assertThat(steinerResultWithReroute.effectiveTargetNodesCount()).isEqualTo(4);

    }

    @Test
    void shouldNotPruneUnprunableNodes() {
        IdFunction invcrossRoadIdFunction = invcrossRoadGraph::toMappedNodeId;

        var steinerResultWithReroute = new ShortestPathsSteinerAlgorithm(
            invcrossRoadGraph,
            invcrossRoadIdFunction.of("a0"),
            List.of(
                invcrossRoadIdFunction.of("a3"),
                invcrossRoadIdFunction.of("a5"),
                invcrossRoadIdFunction.of("a6")
            ),
            2.0,
            1,
            true,
            Pools.DEFAULT,
            ProgressTracker.NULL_TRACKER
        ).compute();
        assertThat(steinerResultWithReroute.totalCost()).isEqualTo(170.0 - 19);
        assertThat(steinerResultWithReroute.effectiveNodeCount()).isEqualTo(6);
        assertThat(steinerResultWithReroute.effectiveTargetNodesCount()).isEqualTo(3);

    }

    @Test
    void shouldTakeAdvantageOfNewSingleParents() {
        IdFunction docsIdFunction = docsGraph::toMappedNodeId;

        var steinerResultWithReroute = new ShortestPathsSteinerAlgorithm(
            docsGraph,
            docsIdFunction.of("a0"),
            List.of(
                docsIdFunction.of("a3"),
                docsIdFunction.of("a4"),
                docsIdFunction.of("a5")
            ),
            2.0,
            1,
            true,
            Pools.DEFAULT,
            ProgressTracker.NULL_TRACKER
        ).compute();
        assertThat(steinerResultWithReroute.totalCost()).isEqualTo(20);

    }

}


