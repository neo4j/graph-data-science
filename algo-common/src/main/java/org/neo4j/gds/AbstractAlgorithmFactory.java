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
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.config.AlgoBaseConfig;
import org.neo4j.graphalgo.core.utils.BatchingProgressLogger;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.progress.ProgressEventTracker;
import org.neo4j.graphalgo.core.utils.progress.v2.tasks.ProgressTracker;
import org.neo4j.graphalgo.core.utils.progress.v2.tasks.TaskProgressTracker;
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
        var progressLogger = loggerFactory.newLogger(
            log,
            taskVolume(graph, configuration),
            taskName(),
            configuration.concurrency(),
            eventTracker
        );
        var progressTracker = new TaskProgressTracker(
            progressTask(graph, configuration),
            progressLogger
        );
        return build(graph, configuration, tracker, progressTracker);
    }

    /**
     * The number of tasks the algorithm will perform. Used for progress logging.
     *
     * @param graph         the graph to compute over
     * @param configuration the configuration of the algorithm
     * @return the number of tasks to perform
     */
    protected abstract long taskVolume(Graph graph, CONFIG configuration);

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
