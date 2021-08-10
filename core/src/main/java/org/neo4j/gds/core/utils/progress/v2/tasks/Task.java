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
package org.neo4j.gds.core.utils.progress.v2.tasks;

import org.apache.commons.lang3.mutable.MutableLong;

import java.util.List;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class Task {

    public static final int UNKNOWN_VOLUME = -1;

    private final String description;
    private final List<Task> subTasks;
    private Status status;

    Task(String description, List<Task> subTasks) {
        this.description = description;
        this.subTasks = subTasks;
        this.status = Status.PENDING;
    }

    public String description() {
        return description;
    }

    List<Task> subTasks() {
        return subTasks;
    }

    public Status status() {
        return this.status;
    }

    public Task nextSubtask() {
        if (subTasks.stream().anyMatch(t -> t.status == Status.RUNNING)) {
            throw new IllegalStateException("Cannot move to next subtask, because some subtasks are still running");
        }

        return subTasks()
            .stream()
            .filter(t -> t.status() == Status.PENDING)
            .findFirst()
            .orElseThrow(() -> new IllegalStateException("No more pending subtasks"));
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
            .progress(progress.getValue())
            .build();
    }

    public void setVolume(long volume) {
        throw new UnsupportedOperationException("Should only be called on a leaf task");
    }

    public void logProgress() {
        logProgress(1);
    }

    public void logProgress(long value) {
        throw new UnsupportedOperationException("Should only be called on a leaf task");
    }

    public void visit(TaskVisitor taskVisitor) {
        taskVisitor.visitIntermediateTask(this);
    }
}
