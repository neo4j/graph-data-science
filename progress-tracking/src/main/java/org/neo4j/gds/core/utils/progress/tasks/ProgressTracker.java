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

import java.util.function.Function;

/**
 * Progress tracking has to do with _events_. Updates to progress are events that happen.
 * _Logging_ that progress is a separate concern, which is not offered here.
 * This interface deals with navigating up and down the task tree, and issuing update events.
 * And then it deals with some legacy breakages of encapsulation, plus some conceptual breakages that are not great.
 * The pattern for using these, is this:
 * <ol>
 *     <li>begin*</li>
 *     <li>on*</li>
 *     <li>...</li>
 *     <li>end*</li>
 *     <li>release</li>
 * </ol>
 * NB: The release call only happens on the outer one
 * Looking at that high level description: it smells a lot like try-with-resources innit - maybe one day...
 */
public interface ProgressTracker {
    ProgressTracker NULL_TRACKER = new NullProgressTracker();

    void beginSubTask();

    void beginSubTask(long taskVolume);

    void beginSubTaskWithSteps(long numberOfSteps);

    void onProgress(long value);

    void onProgress(Function<Long, Long> valueCalculator);

    default void onProgress() {
        onProgress(1);
    }

    void onSteps(long steps);

    void endSubTask();

    void endSubTaskWithFailure();

    void release();
}
