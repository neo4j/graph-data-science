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

class NullProgressTracker implements ProgressTracker {
    @Override
    public void setEstimatedResourceFootprint(MemoryRange memoryRangeInBytes) {
    }

    @Override
    public void requestedConcurrency(Concurrency concurrency) {

    }

    @Override
    public void beginSubTask() {
    }

    @Override
    public void beginSubTask(long taskVolume) {

    }

    @Override
    public void endSubTask() {
    }

    @Override
    public void beginSubTask(String expectedTaskDescription, long taskVolume) {

    }

    @Override
    public void onProgress(long value) {
    }

    @Override
    public void onProgress(Function<Long, Long> valueCalculator) {

    }

    @Override
    public void logSteps(long steps) {

    }

    @Override
    public void release() {
    }

    @Override
    public void endSubTaskWithFailure() {

    }

    @Override
    public void endSubTaskWithFailure(String expectedTaskDescription) {

    }
}
