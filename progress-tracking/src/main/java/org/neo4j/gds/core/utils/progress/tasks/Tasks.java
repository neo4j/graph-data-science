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

import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.mem.MemoryRange;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class Tasks {
    private static final Task EMPTY_TASK = new Task("", new Concurrency(1), List.of());

    public static Task empty() {
        return EMPTY_TASK;
    }

    private Tasks() {}

    public static Task task(String description, Concurrency concurrency, List<Task> children) {
        return new Task(description, concurrency, children);
    }

    public static Task task(String description, Concurrency concurrency, Task firstChild, Task... children) {
        return task(description, concurrency, MemoryRange.empty(), firstChild, children);
    }

    public static Task task(String description, Concurrency concurrency, MemoryRange memoryEstimationInBytes, Task firstChild, Task... children) {
        var childrenList = new ArrayList<Task>();
        childrenList.add(firstChild);
        childrenList.addAll(Arrays.asList(children));
        return new Task(description, concurrency, childrenList, memoryEstimationInBytes);
    }

    public static IterativeTask iterativeFixed(
        String description,
        Concurrency concurrency,
        Supplier<List<Task>> subTasksSupplier,
        int iterations
    ) {
        return new IterativeTask(
            description,
            concurrency,
            unrollTasks(subTasksSupplier, iterations),
            subTasksSupplier,
            IterativeTask.Mode.FIXED
        );
    }

    public static IterativeTask iterativeDynamic(
        String description,
        Concurrency concurrency,
        Supplier<List<Task>> subTasksSupplier,
        int iterations
    ) {
        return iterativeDynamic(description, concurrency, subTasksSupplier, MemoryRange.empty(), iterations);
    }

    public static IterativeTask iterativeDynamic(
        String description,
        Concurrency concurrency,
        Supplier<List<Task>> subTasksSupplier,
        MemoryRange memoryEstimationInBytes,
        int iterations
    ) {
        return new IterativeTask(
            description,
            concurrency,
            unrollTasks(subTasksSupplier, iterations),
            memoryEstimationInBytes,
            subTasksSupplier,
            IterativeTask.Mode.DYNAMIC
        );
    }

    public static IterativeTask iterativeOpen(
        String description,
        Concurrency concurrency,
        Supplier<List<Task>> subTasksSupplier
    ) {
        return new IterativeTask(
            description,
            concurrency,
            // subtasks will be added on the fly, so we need a thread-safe list to allow concurrent reads
            new CopyOnWriteArrayList<>(),
            subTasksSupplier,
            IterativeTask.Mode.OPEN
        );
    }

    public static LeafTask leaf(String description, Concurrency concurrency) {
        return leaf(description, concurrency, Task.UNKNOWN_VOLUME);
    }

    public static LeafTask leaf(String description, Concurrency concurrency, long volume) {
        return new LeafTask(description, concurrency, volume);
    }

    public static Task leaf(String description, Concurrency concurrency, MemoryRange memoryEstimationInBytes) {
        return new LeafTask(description, concurrency, Task.UNKNOWN_VOLUME, memoryEstimationInBytes);
    }

    private static List<Task> unrollTasks(Supplier<List<Task>> subTasksSupplier, int iterations) {
        return Stream.generate(subTasksSupplier).limit(iterations).flatMap(Collection::stream).collect(Collectors.toList());
    }
}
