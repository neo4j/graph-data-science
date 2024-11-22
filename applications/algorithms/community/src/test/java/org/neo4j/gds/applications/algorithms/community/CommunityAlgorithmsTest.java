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
package org.neo4j.gds.applications.algorithms.community;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.applications.algorithms.machinery.ProgressTrackerCreator;
import org.neo4j.gds.compat.TestLog;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.TaskProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;
import org.neo4j.gds.kcore.KCoreDecompositionStreamConfigImpl;
import org.neo4j.gds.logging.GdsTestLog;
import org.neo4j.gds.termination.TerminationFlag;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.gds.assertj.Extractors.removingThreadId;
import static org.neo4j.gds.assertj.Extractors.replaceTimings;

@GdlExtension
class CommunityAlgorithmsTest {

    @GdlExtension
    @Nested
    class KCore {

        @GdlGraph(orientation = Orientation.UNDIRECTED)
        private static final String DB_CYPHER =
            "CREATE " +
                "  (z:node)," +
                "  (a:node)," +
                "  (b:node)," +
                "  (c:node)," +
                "  (d:node)," +
                "  (e:node)," +
                "  (f:node)," +
                "  (g:node)," +
                "  (h:node)," +

                "(a)-[:R]->(b)," +
                "(b)-[:R]->(c)," +
                "(c)-[:R]->(d)," +
                "(d)-[:R]->(e)," +
                "(e)-[:R]->(f)," +
                "(f)-[:R]->(g)," +
                "(g)-[:R]->(h)," +
                "(h)-[:R]->(c)";


        @Inject
        private TestGraph graph;


        @Test
        void shouldLogProgressForKcore() {
            var config = KCoreDecompositionStreamConfigImpl.builder().build();
            var log = new GdsTestLog();

            var progressTrackerCreator = mock(ProgressTrackerCreator.class);
            when(progressTrackerCreator.createProgressTracker(any(), any(Task.class))).then(
                i ->
                    new TaskProgressTracker(
                        i.getArgument(1),
                        log,
                        new Concurrency(4),
                        EmptyTaskRegistryFactory.INSTANCE
                    )
            );

            var algorithms = new CommunityAlgorithms(progressTrackerCreator, TerminationFlag.RUNNING_TRUE);
            algorithms.kCore(graph, config);
            Assertions.assertThat(log.getMessages(TestLog.INFO))
                .extracting(removingThreadId())
                .extracting(replaceTimings())
                .containsExactly(
                    "KCoreDecomposition :: Start",
                    "KCoreDecomposition 11%",
                    "KCoreDecomposition 33%",
                    "KCoreDecomposition 100%",
                    "KCoreDecomposition :: Finished"
                );
        }
    }
}


