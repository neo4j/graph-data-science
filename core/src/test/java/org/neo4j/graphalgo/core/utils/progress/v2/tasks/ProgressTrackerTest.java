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
package org.neo4j.graphalgo.core.utils.progress.v2.tasks;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ProgressTrackerTest {

    @Test
    void shouldStepThroughSubtasks() {
        var leafTask = Tasks.leaf("leaf1");
        var iterativeTask = Tasks.iterativeFixed("iterative", () -> List.of(Tasks.leaf("leaf2")), 2);
        var rootTask = Tasks.task(
            "root",
            leafTask,
            iterativeTask
        );

        var progressTracker = new ProgressTracker(rootTask);

        progressTracker.beginSubTask();
        assertThat(progressTracker.currentSubTask()).isEqualTo(rootTask);

        progressTracker.beginSubTask();
        assertThat(progressTracker.currentSubTask()).isEqualTo(leafTask);
        assertThat(leafTask.status()).isEqualTo(Status.RUNNING);
        progressTracker.endSubTask();
        assertThat(leafTask.status()).isEqualTo(Status.FINISHED);

        progressTracker.beginSubTask();
        assertThat(progressTracker.currentSubTask()).isEqualTo(iterativeTask);
        assertThat(iterativeTask.status()).isEqualTo(Status.RUNNING);
        progressTracker.endSubTask();
        assertThat(iterativeTask.status()).isEqualTo(Status.FINISHED);

        assertThat(progressTracker.currentSubTask()).isEqualTo(rootTask);
    }

    @Test
    void shouldThrowIfEndMoreTasksThanStarted() {
        var task = Tasks.leaf("leaf");
        var progressTracker = new ProgressTracker(task);
        progressTracker.beginSubTask();
        progressTracker.endSubTask();
        assertThatThrownBy(progressTracker::endSubTask)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("No more running tasks");
    }

    @Test
    void shouldLogProgress() {
        var task = Tasks.leaf("leaf");
        var progressTracker = new ProgressTracker(task);
        progressTracker.beginSubTask();
        progressTracker.logProgress(42);
        assertThat(task.getProgress().progress()).isEqualTo(42);
    }
}
