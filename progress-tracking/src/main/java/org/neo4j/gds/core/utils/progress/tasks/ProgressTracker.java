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

import java.util.function.Function;

/**
 * Progress tracking has to do with _events_. Updates to progress are events that happen.
 * _Logging_ that progress is a separate concern, which is not offered here.
 * This interface deals with navigating up and down the task tree, and issuing update events.
 * And then it deals with some legacy breakages of encapsulation, plus some conceptual breakages that are not great.
 */
public interface ProgressTracker {
    ProgressTracker NULL_TRACKER = new NullProgressTracker();

    void beginSubTask();

    void beginSubTask(long taskVolume);

    void beginSubTask(String expectedTaskDescription);

    void beginSubTask(String expectedTaskDescription, long taskVolume);

    void onProgress(long value);

    void onProgress(Function<Long, Long> valueCalculator);

    default void onProgress() {
        onProgress(1);
    }

    void onProgress(long value, String messageTemplate);

    void endSubTask();

    void endSubTask(String expectedTaskDescription);

    void endSubTaskWithFailure();

    void endSubTaskWithFailure(String expectedTaskDescription);

    void release();

    void setSteps(long steps);

    void logSteps(long steps);

    /*
     * Conceptual breakages - where we make our code less cohesive by coupling unrelated things.
     */

    /**
     * This method exists so that the old framework that powers Pregel,
     * can set a memory related piece of metadata on the root task in the task tree.
     * That metadata in turn is used for display in some UI.
     * And it exists here because for some reason,
     * the root task in the task tree could not just be injected into that code.
     * So this method call passes through dumbly to the root task which just happens to sit on the progress tracker.
     * Incidental coupling, and a pain to change.
     */
    void setEstimatedResourceFootprint(MemoryRange memoryEstimationInBytes);

    /**
     * This method exists so that the old framework that powers Pregel,
     * can set a related piece of metadata on the root task in the task tree.
     * That metadata in turn is used for display in some UI.
     * And it exists here because for some reason,
     * the root task in the task tree could not just be injected into that code.
     * So this method call passes through dumbly to the root task which just happens to sit on the progress tracker.
     * Incidental coupling, and a pain to change.
     */
    @Deprecated
    void requestedConcurrency(Concurrency concurrency);
}
