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

import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.compat.TestLog;
import org.neo4j.gds.core.utils.progress.GlobalTaskStore;
import org.neo4j.gds.core.utils.progress.JobId;
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.TaskStore;
import org.neo4j.gds.core.utils.progress.tasks.Progress;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.TaskProgressTracker;
import org.neo4j.gds.core.utils.warnings.EmptyUserLogRegistryFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.neo4j.gds.core.utils.progress.tasks.Task.UNKNOWN_VOLUME;

public class InspectableTestProgressTracker extends TaskProgressTracker {

    private final TaskStore taskStore;
    private final TestLog log;
    private final JobId jobId;
    private final String userName;
    private final List<Optional<Progress>> progressHistory = new ArrayList<>();

    public InspectableTestProgressTracker(Task baseTask, String userName, JobId jobId) {
        this(baseTask, userName, jobId, new GlobalTaskStore(), Neo4jProxy.testLog());
    }

    private InspectableTestProgressTracker(Task baseTask, String userName, JobId jobId, TaskStore taskStore, TestLog log) {
        super(
            baseTask,
            log,
            1,
            jobId,
            TaskRegistryFactory.local(userName, taskStore),
            EmptyUserLogRegistryFactory.INSTANCE
        );
        baseTask.getProgress();
        this.userName = userName;
        this.jobId = jobId;
        this.taskStore = taskStore;
        this.log = log;
    }

    public TestLog log() {
        return log;
    }

    @Override
    public void logProgress(long progress) {
        super.logProgress(progress);
    }

    @Override
    public void beginSubTask() {
        super.beginSubTask();
        progressHistory.add(taskStore.query(userName, jobId).map(userTask -> userTask.task().getProgress()));
    }

    @Override
    public void endSubTask() {
        super.endSubTask();
        progressHistory.add(taskStore.query(userName, jobId).map(userTask -> userTask.task().getProgress()));
    }

    @Override
    public void setVolume(long volume) {
        super.setVolume(volume);
    }

    public void assertValidProgressEvolution() {
        assertThat(progressHistory).isNotEmpty();
        assertThat(progressHistory.get(0)).isPresent();
        var previousProgress = progressHistory.get(0).get();
        var initialVolume = previousProgress.volume();
        assertThat(initialVolume).isNotEqualTo(UNKNOWN_VOLUME);
        assertThat(previousProgress.progress()).isEqualTo(0);
        for (Optional<Progress> maybeProgress : progressHistory.subList(1, progressHistory.size())) {
            if (maybeProgress.isPresent()) {
                var progress = maybeProgress.get();
                assertThat(progress.volume()).isEqualTo(initialVolume);
                assertThat(progress.progress()).isGreaterThanOrEqualTo(previousProgress.progress());
                previousProgress = progress;
            }
        }
        assertThat(previousProgress.progress()).isEqualTo(previousProgress.volume());
    }
}
