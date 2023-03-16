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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class Tasks {

    private static final Task EMPTY_TASK = new Task("", List.of());

    public static Task empty() {
        return EMPTY_TASK;
    }

    public static Task task(String description, List<Task> children) {
        return new Task(description, children);
    }

    public static Task task(String description, Task firstChild, Task... children) {
        var childrenList = new ArrayList<Task>();
        childrenList.add(firstChild);
        childrenList.addAll(Arrays.asList(children));
        return new Task(description, childrenList);
    }

    public static IterativeTask iterativeFixed(
        String description,
        Supplier<List<Task>> subTasksSupplier,
        int iterations
    ) {
        return new IterativeTask(
            description,
            unrollTasks(subTasksSupplier, iterations),
            subTasksSupplier,
            IterativeTask.Mode.FIXED
        );
    }

    public static IterativeTask iterativeDynamic(
        String description,
        Supplier<List<Task>> subTasksSupplier,
        int iterations
    ) {
        return new IterativeTask(
            description,
            unrollTasks(subTasksSupplier, iterations),
            subTasksSupplier,
            IterativeTask.Mode.DYNAMIC
        );
    }

    public static IterativeTask iterativeOpen(
        String description,
        Supplier<List<Task>> subTasksSupplier
    ) {
        return new IterativeTask(
            description,
            new ArrayList<>(),
            subTasksSupplier,
            IterativeTask.Mode.OPEN
        );
    }

    public static LeafTask leaf(String description) {
        return leaf(description, Task.UNKNOWN_VOLUME);
    }

    public static LeafTask leaf(String description, long volume) {
        return new LeafTask(description, volume);
    }

    private static List<Task> unrollTasks(Supplier<List<Task>> subTasksSupplier, int iterations) {
        return Stream.generate(subTasksSupplier).limit(iterations).flatMap(Collection::stream).collect(Collectors.toList());
    }

    private Tasks() {}
}
