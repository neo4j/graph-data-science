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

import org.neo4j.gds.core.utils.ProgressLogger;
import org.neo4j.gds.core.utils.progress.EmptyProgressEventTracker;
import org.neo4j.gds.core.utils.progress.ProgressEventTracker;

public interface ProgressTracker {

    ProgressTracker NULL_TRACKER = new EmptyProgressTracker();

    void beginSubTask();

    void endSubTask();

    void logProgress(long value);

    default void logProgress() {
        logProgress(1);
    }

    void setVolume(long volume);

    ProgressLogger progressLogger();

    ProgressEventTracker progressEventTracker();

    void release();

    class EmptyProgressTracker implements ProgressTracker {
        @Override
        public void beginSubTask() {
        }

        @Override
        public void endSubTask() {
        }

        @Override
        public void logProgress(long value) {
        }

        @Override
        public void setVolume(long volume) {
        }

        @Override
        public ProgressLogger progressLogger() {
            return ProgressLogger.NULL_LOGGER;
        }

        @Override
        public ProgressEventTracker progressEventTracker() {
            return EmptyProgressEventTracker.INSTANCE;
        }

        @Override
        public void release() {
        }
    }
}
