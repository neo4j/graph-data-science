/*
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
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
package org.neo4j.graphalgo.impl;

import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.logging.Log;

/**
 * @author mknblch
 */
public abstract class Algorithm<ME extends Algorithm<ME>> implements TerminationFlag {

    protected ProgressLogger progressLogger = ProgressLogger.NULL_LOGGER;

    protected TerminationFlag terminationFlag = TerminationFlag.RUNNING_TRUE;

    public abstract ME me();

    public abstract ME release();

    public ME withLog(Log log) {
        return withProgressLogger(ProgressLogger.wrap(log, getClass().getSimpleName()));
    }

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
        return progressLogger;
    }

    @Override
    public boolean running() {
        return terminationFlag.running();
    }
}
