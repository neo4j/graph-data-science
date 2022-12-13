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
    @GdlGraph(orientation = Orientation.NATURAL)
    private static final String DB_CYPHER =
        "CREATE " +
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
    
    @Inject
    private TestGraph graph;

    @Inject
    private IdFunction idFunction;

    @GdlGraph(graphNamePrefix = "noReroute")
    private static final String nodeCreateQuery =
        "CREATE " +
        "  (a0:Node)," +
        "  (a1:Node)," +
        "  (a2:Node)," +
        "  (a3:Node)," +
        "  (a0)-[:R {weight: 1.0}]->(a1)," +
        "  (a1)-[:R {weight: 1.0}]->(a2)," +
        "  (a2)-[:R {weight: 1.0}]->(a3)," +
        "  (a3)-[:R {weight: 0.5}]->(a1),";

    @Inject
    private TestGraph noRerouteGraph;

    @Inject
    private IdFunction noRerouteIdFunction;


    @Test
    void shouldPruneUnusedIfRerouting() {
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
    void rerouteShouldNotCreateLoops() {
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

        assertThat(parent[(int) idFunction.of("a0")]).isEqualTo(ShortestPathsSteinerAlgorithm.ROOTNODE);
        assertThat(parent[(int) idFunction.of("a1")]).isEqualTo(idFunction.of("a0"));
        assertThat(parent[(int) idFunction.of("a2")]).isEqualTo(idFunction.of("a1"));
        assertThat(parent[(int) idFunction.of("a3")]).isEqualTo(idFunction.of("a2"));

        assertThat(steinerResult.totalCost()).isEqualTo(3);

    }

    @Test
    void shouldWorkForUnreachableAndReachableTerminals() {

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

        var sourceId = graph.toOriginalNodeId(idFunction.of("a0"));
        var target1 = graph.toOriginalNodeId(idFunction.of("a3"));
        var target2 = graph.toOriginalNodeId(idFunction.of("a4"));

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
                "SteinerTree 50%",
                "SteinerTree 100%",
                "SteinerTree :: Finished"
            );
    }

    @Test
    void shouldLogProgressWithRerouting() {

        var sourceId = graph.toOriginalNodeId(idFunction.of("a0"));
        var target1 = graph.toOriginalNodeId(idFunction.of("a3"));
        var target2 = graph.toOriginalNodeId(idFunction.of("a4"));

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
                "SteinerTree :: Main :: Start",
                "SteinerTree :: Main 50%",
                "SteinerTree :: Main 100%",
                "SteinerTree :: Main :: Finished",
                "SteinerTree :: Rerouting :: Start",
                "SteinerTree :: Rerouting 16%",
                "SteinerTree :: Rerouting 33%",
                "SteinerTree :: Rerouting 50%",
                "SteinerTree :: Rerouting 66%",
                "SteinerTree :: Rerouting 83%",
                "SteinerTree :: Rerouting 100%",
                "SteinerTree :: Rerouting :: Finished",
                "SteinerTree :: Finished"
            );
    }

}
