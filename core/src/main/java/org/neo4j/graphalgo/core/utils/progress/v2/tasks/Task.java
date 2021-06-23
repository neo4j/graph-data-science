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

import org.apache.commons.lang3.mutable.MutableLong;

import java.util.List;

public class Task {

    private final String description;
    private final List<Task> subTasks;
    private Status status;

    Task(String description, List<Task> subTasks) {
        this.description = description;
        this.subTasks = subTasks;
        this.status = Status.OPEN;
    }

    public Progress getProgress() {
        var volume = new MutableLong(0);
        var progress = new MutableLong(0);

        subTasks().stream().map(Task::getProgress).forEach(childProgress -> {
            if (childProgress.volume() < 0 || volume.getValue() < 0) {
                volume.setValue(-1);
            } else {
                volume.add(childProgress.volume());
            }

            progress.add(childProgress.progress());
        });

        return ImmutableProgress.of(volume.getValue(), progress.getValue());
    }

    public String description() {
        return description;
    }

    public Task nextSubtask() {
        return subTasks().stream().filter(t -> t.status() == Status.OPEN).findFirst().get();
    }

    List<Task> subTasks() {
        return subTasks;
    }

    public Status status() {
        return this.status;
    }

    public void start() {
        this.status = Status.RUNNING;
    }

    public void finish() {
        this.status = Status.FINISHED;
    }

    public void cancel() {
        this.status = Status.CANCELED;
    }

    public void logProgress() {
        throw new UnsupportedOperationException("Should only be called on a leave task");
    }
}
