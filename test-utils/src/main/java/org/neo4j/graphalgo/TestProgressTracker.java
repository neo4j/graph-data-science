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
package org.neo4j.graphalgo;

import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.progress.v2.tasks.ProgressTracker;

public class TestProgressTracker implements ProgressTracker {

    private final ProgressLogger progressLogger;

    public TestProgressTracker(ProgressLogger progressLogger) {
        this.progressLogger = progressLogger;
    }

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
        return progressLogger;
    }

    @Override
    public void release() {

    }
}
