/*
 * Copyright (c) 2017-2019 "Neo4j,"
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

import org.neo4j.graphalgo.core.utils.mem.Assessable;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;
import org.neo4j.graphalgo.core.utils.mem.MemoryTree;
import org.neo4j.logging.Log;

/**
 * @author mknblch
 */
public abstract class Algorithm<ME extends Algorithm<ME>> implements TerminationFlag, Assessable {

    protected ProgressLogger progressLogger = ProgressLogger.NULL_LOGGER;

    protected TerminationFlag terminationFlag = TerminationFlag.RUNNING_TRUE;

    public abstract ME me();

    public abstract ME release();

    /**
     * Returns an estimation about the memory consumption of that algorithm. The memory estimation can be used to
     * compute the actual consumption depending on {@link GraphDimensions} and concurrency.
     *
     * @return memory estimation
     * @see MemoryEstimations
     * @see MemoryEstimation#estimate(GraphDimensions, int)
     */
    public MemoryEstimation memoryEstimation() {
        return MemoryEstimations.empty();
    }

    /**
     * Computes the memory consumption for the algorithm depending on the given {@link GraphDimensions} and concurrency.
     *
     * This is shorthand for {@link MemoryEstimation#estimate(GraphDimensions, int)}.
     *
     * @param dimensions  graph dimensions
     * @param concurrency concurrency which is used to run the algorithm
     * @return memory requirements
     */
    public MemoryTree memoryEstimation(GraphDimensions dimensions, int concurrency) {
        return memoryEstimation().estimate(dimensions, concurrency);
    }

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
