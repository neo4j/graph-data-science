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
package org.neo4j.gds.progress.logging;

import org.neo4j.gds.progress.tasks.Task;

/**
 * Progress logging follows the structure of progress tracking:
 *
 * <ol>
 *     <li>Begin a subtask</li>
 *     <li>Log some progress</li>
 *     <li>End the subtask</li>
 * </ol>
 *
 * Of course, it can work nested-ly:
 *
 * <ol>
 *     <li>Begin a subtask</li>
 *     <li>Log some progress</li>
 *     <li>Begin a nested subtask</li>
 *     <li>Log some progress</li>
 *     <li>End the nested subtask</li>
 *     <li>Log some progress</li>
 *     <li>End the subtask</li>
 * </ol>
 *
 * Remember in the latter case that sometimes you might need to ise the reset method.
 */
public interface ProgressLogger {
    void logBeginSubTask(Task task, Task parentTask);

    void logProgress(long progress);

    void logEndSubTask(Task task, Task parentTask);

    void logEndSubTaskWithFailure(Task task, Task parentTask);

    long reset(long newTaskVolume);

    void release();
}
