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
package org.neo4j.gds.core.utils.progress.tasks;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.compat.TestLog;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.assertj.Extractors.removingThreadId;

class TaskProgressTrackerFailMethodTest {

    @Test
    void failingTask() {
        var failingTask = Tasks.leaf("failingTask");
        var log = Neo4jProxy.testLog();
        var tracker = new TaskProgressTracker(failingTask, log, 1, EmptyTaskRegistryFactory.INSTANCE);

        tracker.beginSubTask();
        tracker.endSubTaskWithFailure();

        assertThat(log.getMessages(TestLog.INFO))
            .extracting(removingThreadId())
            .containsExactly(
                "failingTask :: Start",
                "failingTask 100%",
                "failingTask :: Failed"
            );
    }

    @Test
    void failingIntermediateTask() {

        var failingSubTask = Tasks.leaf("failingSubTask");

        var rootTask = Tasks.task("rootTask", failingSubTask);
        var log = Neo4jProxy.testLog();
        var tracker = new TaskProgressTracker(rootTask, log, 1, EmptyTaskRegistryFactory.INSTANCE);

        tracker.beginSubTask("rootTask");
        tracker.beginSubTask("failingSubTask");
        tracker.endSubTaskWithFailure("failingSubTask");

        assertThat(log.getMessages(TestLog.INFO))
            .extracting(removingThreadId())
            .containsExactly(
                "rootTask :: Start",
                "rootTask :: failingSubTask :: Start",
                "rootTask :: failingSubTask 100%",
                "rootTask :: failingSubTask :: Failed",
                "rootTask :: Failed"
            );
    }

}
