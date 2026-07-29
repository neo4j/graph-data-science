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
package org.neo4j.gds.progress.registration;

import org.neo4j.gds.progress.tasks.Task;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Here we hold tasks for a {database, user, job} triplet.
 * There can be multiple tasks (roots of task trees), for example when you have an algorithm followed by write mode.
 * That would be two task trees, but under the same job.
 * We promise to keep the tasks in insertion order.
 * ~~We probably achieve that by assigning internal, sequential task ids~~no actually, a list was enough.
 */
class TaskContainer {
    // The list of tasks is extremely short, so COW seems fine. It is in response to an observed concurrency issue
    private final List<Task> tasks = new CopyOnWriteArrayList<>();

    void add(Task task) {
        tasks.add(task);
    }

    void removeAll() {
        tasks.clear();
    }

    Collection<Task> markAllCompleted() {
        tasks.forEach(task -> {
            switch (task.status()) {
                case PENDING -> task.cancel();
                case RUNNING -> task.finish();
            }
        });

        return tasks;
    }

    /**
     * @return all tasks in insertion order
     */
    List<Task> getAll() {
        return tasks;
    }
}
