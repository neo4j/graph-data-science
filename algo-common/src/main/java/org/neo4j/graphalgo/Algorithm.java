/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.logging.Log;

public abstract class Algorithm<ME extends Algorithm<ME, RESULT>, RESULT> implements TerminationFlag {
    protected ProgressLogger progressLogger = ProgressLogger.NULL_LOGGER;

    protected TerminationFlag terminationFlag = TerminationFlag.RUNNING_TRUE;

    public abstract RESULT compute();

    public abstract ME me();

    /**
     * Release internal data structures used by the algorithm.
     *
     */
    public abstract void release();

    @Deprecated
    // This is kept for alpha algorithms using the ProgressLoggerAdapter
    public ME withProgressLogger(ProgressLogger progressLogger) {
        this.progressLogger = progressLogger;
        return me();
    }

    public ME withTerminationFlag(TerminationFlag terminationFlag) {
        this.terminationFlag = terminationFlag;
        return me();
    }

    public TerminationFlag getTerminationFlag() {
        return terminationFlag;
    }

    public ProgressLogger getProgressLogger() {
        return this.progressLogger;
    };

    @Override
    public boolean running() {
        return terminationFlag.running();
    }
}
