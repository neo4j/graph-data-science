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
package org.neo4j.gds.nodecount;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.MiscellaneousAlgorithmsTasks;
import org.neo4j.gds.TestGraph;
import org.neo4j.gds.TestProgressTrackerHelper;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.termination.TerminationFlag;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.gds.assertj.Extractors.removingThreadId;
import static org.neo4j.gds.compat.TestLog.INFO;

@GdlExtension
class NodeCountTest {

    @GdlGraph
    static String GDL =
        "(a)-->(b)" +
        ",(b)-->(c)" +
        ",(c)-->(a)";

    @Inject
    private TestGraph graph;

    @Test
    void countsNodes() {
        var algorithm = new NodeCount(graph, ProgressTracker.NULL_TRACKER, TerminationFlag.RUNNING_TRUE);

        var result = algorithm.compute();

        assertEquals(graph.nodeCount(), result.nodeCount());
        assertEquals(3L, result.nodeCount());
    }

    @Test
    void progressLogging() {
        var progressTrackerWithLog = TestProgressTrackerHelper.create(
            MiscellaneousAlgorithmsTasks.nodeCount(graph),
            new Concurrency(1)
        );

        var progressTracker = progressTrackerWithLog.progressTracker();
        var log = progressTrackerWithLog.log();

        new NodeCount(graph, progressTracker, TerminationFlag.RUNNING_TRUE).compute();

        Assertions.assertThat(log.getMessages(INFO))
            // avoid asserting on the thread id
            .extracting(removingThreadId())
            .contains(
                "NodeCount :: Start",
                "NodeCount :: Finished"
            );
    }
}
