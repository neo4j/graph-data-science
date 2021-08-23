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
package org.neo4j.gds;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class SubTaskCountingVisitorTest {

    @Test
    void shouldCountSimpleTask() {
        var leaf1 = Tasks.leaf("leaf1");
        var baseTask = Tasks.task("base", leaf1, Tasks.leaf("leaf2"));

        var subTaskCountingVisitor = new ListProgressProc.SubTaskCountingVisitor();
        baseTask.visit(subTaskCountingVisitor);

        assertThat(subTaskCountingVisitor.numSubTasks()).isEqualTo(3);
        assertThat(subTaskCountingVisitor.numFinishedSubTasks()).isEqualTo(0);

        leaf1.start();
        leaf1.finish();

        subTaskCountingVisitor.reset();
        baseTask.visit(subTaskCountingVisitor);
        assertThat(subTaskCountingVisitor.numSubTasks()).isEqualTo(3);
        assertThat(subTaskCountingVisitor.numFinishedSubTasks()).isEqualTo(1);
    }

    @Test
    void shouldCountIterativeTasks() {
        var baseTask = Tasks.task(
            "base",
            Tasks.iterativeFixed("iterative", () -> List.of(Tasks.leaf("leaf1"), Tasks.leaf("leaf2")), 2)
        );

        var subTaskCountingVisitor = new ListProgressProc.SubTaskCountingVisitor();
        baseTask.visit(subTaskCountingVisitor);

        assertThat(subTaskCountingVisitor.numSubTasks()).isEqualTo(6);
        assertThat(subTaskCountingVisitor.numFinishedSubTasks()).isEqualTo(0);

        var iterativeTask = baseTask.nextSubtask();
        var leaf1Iteration1 = iterativeTask.nextSubtask();
        leaf1Iteration1.start();
        leaf1Iteration1.finish();
        var leaf2Iteration1 = iterativeTask.nextSubtask();
        leaf2Iteration1.start();
        leaf2Iteration1.finish();

        subTaskCountingVisitor.reset();
        baseTask.visit(subTaskCountingVisitor);

        assertThat(subTaskCountingVisitor.numSubTasks()).isEqualTo(6);
        assertThat(subTaskCountingVisitor.numFinishedSubTasks()).isEqualTo(2);
    }

    @Test
    void shouldDetectOpenIterativeTask() {
        var baseTask = Tasks.iterativeOpen("open", () -> List.of(Tasks.leaf("leaf")));

        var subTaskCountingVisitor = new ListProgressProc.SubTaskCountingVisitor();
        baseTask.visit(subTaskCountingVisitor);

        assertThat(subTaskCountingVisitor.numSubTasks()).isEqualTo(1);
        assertThat(subTaskCountingVisitor.numFinishedSubTasks()).isEqualTo(0);
        assertThat(subTaskCountingVisitor.containsOpenTask()).isTrue();
    }
}
