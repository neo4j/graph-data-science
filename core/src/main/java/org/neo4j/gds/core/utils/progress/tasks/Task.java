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

import org.apache.commons.lang3.mutable.MutableLong;
import org.neo4j.gds.core.utils.ClockService;
import org.neo4j.gds.core.utils.mem.MemoryRange;

import java.util.List;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class Task {

    public static final int UNKNOWN_VOLUME = -1;
    public static final int UNKNOWN_CONCURRENCY = -1;
    public static final long NOT_STARTED = -1L;
    public static final long NOT_FINISHED = -1L;

    private final String description;
    private final List<Task> subTasks;
    private Status status;
    private long startTime;
    private long finishTime;

    private MemoryRange estimatedMemoryRangeInBytes = MemoryRange.empty();
    private int maxConcurrency = UNKNOWN_CONCURRENCY;

    Task(String description, List<Task> subTasks) {
        this.description = description;
        this.subTasks = subTasks;
        this.status = Status.PENDING;
        this.startTime = NOT_STARTED;
        this.finishTime = NOT_FINISHED;
    }

    public String description() {
        return description;
    }

    public List<Task> subTasks() {
        return subTasks;
    }

    public Status status() {
        return this.status;
    }

    public Task nextSubtask() {
        validateTaskIsRunning();
        return nextSubTaskAfterValidation();
    }

    public void start() {
        if (this.status != Status.PENDING) {
            throw new UnsupportedOperationException(formatWithLocale(
                "Task `%s` with state %s cannot be started",
                this.description,
                this.status
            ));
        }
        this.status = Status.RUNNING;
        this.startTime = ClockService.clock().millis();
    }

    public void finish() {
        if (this.status != Status.RUNNING) {
            throw new UnsupportedOperationException(formatWithLocale(
                "Task `%s` with state %s cannot be finished",
                this.description,
                this.status
            ));
        }
        this.status = Status.FINISHED;
        this.finishTime = ClockService.clock().millis();
    }

    public void cancel() {
        if (this.status == Status.FINISHED) {
            throw new UnsupportedOperationException(formatWithLocale(
                "Task `%s` with state %s cannot be canceled",
                this.description,
                this.status
            ));
        }
        this.status = Status.CANCELED;
    }

    public Progress getProgress() {
        var volume = new MutableLong(0);
        var progress = new MutableLong(0);

        subTasks().stream().map(Task::getProgress).forEach(childProgress -> {
            if (childProgress.volume() == UNKNOWN_VOLUME || volume.getValue() == UNKNOWN_VOLUME) {
                volume.setValue(UNKNOWN_VOLUME);
            } else {
                volume.add(childProgress.volume());
            }

            progress.add(childProgress.progress());
        });

        return ImmutableProgress.builder()
            .volume(volume.getValue())
            // safe guard to avoid invalid progress logging
            .progress(Math.min(volume.getValue(), progress.getValue()))
            .build();
    }

    public void setVolume(long volume) {
        throw new UnsupportedOperationException(formatWithLocale(
            "Should only be called on a leaf task, but task `%s` is not a leaf",
            description
        ));
    }

    public void logProgress() {
        logProgress(1);
    }

    public void logProgress(long value) {
        throw new UnsupportedOperationException(formatWithLocale(
            "Should only be called on a leaf task, but task `%s` is not a leaf",
            description
        ));
    }

    public void visit(TaskVisitor taskVisitor) {
        taskVisitor.visitIntermediateTask(this);
    }

    public long startTime() {
        return this.startTime;
    }

    public long finishTime() {
        return this.finishTime;
    }

    public boolean hasNotStarted() {
        return status() == Status.PENDING || startTime() == Task.NOT_STARTED;
    }

    public MemoryRange estimatedMemoryRangeInBytes() {
        return this.estimatedMemoryRangeInBytes;
    }

    public int maxConcurrency() {
        return this.maxConcurrency;
    }

    public void setMaxConcurrency(int maxConcurrency) {
        this.maxConcurrency = maxConcurrency;
        subTasks.forEach(task -> {
            if (task.maxConcurrency() == UNKNOWN_CONCURRENCY) {
                task.setMaxConcurrency(maxConcurrency);
            }
        });
    }

    public void setEstimatedMemoryRangeInBytes(MemoryRange memoryRangeInBytes) {
        this.estimatedMemoryRangeInBytes = memoryRangeInBytes;
    }

    public void fail() {
        this.status = Status.FAILED;
    }

    protected Task nextSubTaskAfterValidation() {
        if (subTasks.stream().anyMatch(t -> t.status == Status.RUNNING)) {
            throw new IllegalStateException("Cannot move to next subtask, because some subtasks are still running");
        }

        return subTasks()
            .stream()
            .filter(t -> t.status() == Status.PENDING)
            .findFirst()
            .orElseThrow(() -> new IllegalStateException("No more pending subtasks"));
    }

    private void validateTaskIsRunning() {
        if (this.status != Status.RUNNING) {
            throw new IllegalStateException(formatWithLocale("Cannot retrieve next subtask, task `%s` is not running.", description()));
        }
    }

    public String render() {
        StringBuilder sb = new StringBuilder();
        render(sb, this, 0);
        return sb.toString();
    }

    static void render(StringBuilder sb, Task task, int depth) {
        sb.append("\t".repeat(Math.max(0, depth - 1)));

        if (depth > 0) {
            sb.append("|-- ");
        }

        sb.append(task.description)
            .append('(')
            .append(task.status)
            .append(')')
            .append(System.lineSeparator());

        task.subTasks().forEach(subtask -> render(sb, subtask, depth + 1));
    }
}
