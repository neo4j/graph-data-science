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
package org.neo4j.gds.articulationpoints;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.CentralityAlgorithmTasks;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.TestProgressTrackerHelper;
import org.neo4j.gds.articulationPoints.ArticulationPointsParameters;
import org.neo4j.gds.compat.TestLog;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.assertj.Extractors.removingThreadId;
import static org.neo4j.gds.assertj.Extractors.replaceTimings;

@GdlExtension
class ArticulationPointsTest {

    @GdlGraph(orientation = Orientation.UNDIRECTED)
    private static String GRAPH =
        """
                CREATE
                  (a:Node),
                  (b:Node),
                  (c:Node),
                  (d:Node),
                  (e:Node),
                  (f:Node),

                  (a)-[:REL]->(c),
                  (c)-[:REL]->(b),
                  (d)-[:REL]->(a),
                  (e)-[:REL]->(b),
                  (e)-[:REL]->(c),
                  (f)-[:REL]->(a),
                  (f)-[:REL]->(c),
                  (f)-[:REL]->(d)
            """;

    @Inject
    private TestGraph graph;

    @Test
    void articulationPoints() {

        var articulationPoints =  ArticulationPoints.create(
            graph,
            new ArticulationPointsParameters(null, false),
            ProgressTracker.NULL_TRACKER
        );

        var result = articulationPoints.compute().articulationPoints();

        assertThat(result)
            .isNotNull()
            .satisfies(bitSet -> {
                assertThat(bitSet.get(graph.toMappedNodeId("c")))
                    .as("Node `c` should be an articulation point.")
                    .isTrue();
                assertThat(bitSet.cardinality())
                    .as("There should be only one articulation point.")
                    .isEqualTo(1L);
            });
    }

    @Test
    void shouldLogProgress(){

        var progressTrackerWithLog = TestProgressTrackerHelper.create(
            new CentralityAlgorithmTasks().articulationPoints(graph),
            new Concurrency(1)
        );

        var progressTracker = progressTrackerWithLog.progressTracker();
        var log = progressTrackerWithLog.log();

        var articulationPoints =  ArticulationPoints.create(
            graph,
            new ArticulationPointsParameters(null, false),
            progressTracker
        );

        articulationPoints.compute();

        Assertions.assertThat(log.getMessages(TestLog.INFO))
            .extracting(removingThreadId())
            .extracting(replaceTimings())
            .containsExactly(
                "ArticulationPoints :: Start",
                "ArticulationPoints 16%",
                "ArticulationPoints 33%",
                "ArticulationPoints 50%",
                "ArticulationPoints 66%",
                "ArticulationPoints 83%",
                "ArticulationPoints 100%",
                "ArticulationPoints :: Finished"
            );
    }

}
