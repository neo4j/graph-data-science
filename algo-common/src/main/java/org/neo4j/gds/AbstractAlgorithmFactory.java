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
package org.neo4j.gds;

import org.jetbrains.annotations.TestOnly;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.core.utils.BatchingProgressLogger;
import org.neo4j.gds.core.utils.ProgressLogger;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.progress.ProgressEventTracker;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.TaskProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.exceptions.MemoryEstimationNotImplementedException;
import org.neo4j.logging.Log;

public abstract class AbstractAlgorithmFactory<ALGO extends Algorithm<ALGO, ?>, CONFIG extends AlgoBaseConfig> implements AlgorithmFactory<ALGO, CONFIG> {

    private final ProgressLogger.ProgressLoggerFactory loggerFactory;

    protected AbstractAlgorithmFactory() {
        this(BatchingProgressLogger.FACTORY);
    }

    @TestOnly
    protected AbstractAlgorithmFactory(ProgressLogger.ProgressLoggerFactory loggerFactory) {
        this.loggerFactory = loggerFactory;
    }

    @Override
    public final ALGO build(
        Graph graph,
        CONFIG configuration,
        AllocationTracker tracker,
        Log log,
        ProgressEventTracker eventTracker
    ) {
        var progressTask = progressTask(graph, configuration);
        var progressLogger = loggerFactory.newLogger(
            log,
            progressTask,
            configuration.concurrency()
        );
        var progressTracker = new TaskProgressTracker(
            progressTask,
            progressLogger,
            eventTracker
        );
        return build(graph, configuration, tracker, progressTracker);
    }

    /**
     * Returns an estimation about the memory consumption of that algorithm. The memory estimation can be used to
     * compute the actual consumption depending on {@link org.neo4j.gds.core.GraphDimensions} and concurrency.
     *
     * @return memory estimation
     * @see org.neo4j.gds.core.utils.mem.MemoryEstimations
     * @see org.neo4j.gds.core.utils.mem.MemoryEstimation#estimate(org.neo4j.gds.core.GraphDimensions, int)
     */
    public MemoryEstimation memoryEstimation(CONFIG configuration) {
        throw new MemoryEstimationNotImplementedException();
    }

    protected Task progressTask(Graph graph, CONFIG config) {
        return Tasks.leaf(taskName());
    }

    /**
     * The name of the task. Typically the name of the algorithm, but Java type params are not good enough.
     * Used for progress logging.
     *
     * @return the name of the task that logs progress
     */
    protected abstract String taskName();

    /**
     * Builds the algorithm class.
     */
    protected abstract ALGO build(
        Graph graph,
        CONFIG configuration,
        AllocationTracker tracker,
        ProgressTracker progressTracker
    );
}
