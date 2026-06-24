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

import org.apache.commons.lang3.tuple.Pair;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.core.PlainSimpleRequestCorrelationId;
import org.neo4j.gds.core.utils.logging.LoggerForProgressTrackingAdapter;
import org.neo4j.gds.progress.tracking.ProgressTrackerFactory;
import org.neo4j.gds.progress.registration.TaskRegistryFactory;
import org.neo4j.gds.progress.tracking.ProgressTracker;
import org.neo4j.gds.progress.tasks.Task;
import org.neo4j.gds.progress.tasks.Tasks;
import org.neo4j.gds.exceptions.MemoryEstimationNotImplementedException;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.mem.MemoryEstimation;
import org.neo4j.gds.mem.MemoryRange;
import org.neo4j.gds.termination.TerminationFlag;

public interface AlgorithmFactory<G, ALGO extends Algorithm<?>, CONFIG extends AlgoBaseConfig> {
    default Pair<ALGO, ProgressTracker> build(
        G graphOrGraphStore,
        CONFIG configuration,
        Log log,
        TaskRegistryFactory taskRegistryFactory,
        TerminationFlag terminationFlag,
        MemoryRange memoryEstimationInBytes
    ) {
        var progressTask = progressTask(graphOrGraphStore, configuration, memoryEstimationInBytes);

        var progressTracker = createProgressTracker(
            configuration,
            log,
            taskRegistryFactory,
            progressTask
        );

        return Pair.of(build(graphOrGraphStore, configuration, progressTracker, terminationFlag), progressTracker);
    }

    private ProgressTracker createProgressTracker(
        CONFIG configuration,
        Log log,
        TaskRegistryFactory taskRegistryFactory,
        Task progressTask
    ) {
        /*
         * For Pregel stuff I am going to shortcut request correlation.
         * That old code is a bridge too far.
         * And you do get the benefit of mostly having correlation, just using job id,
         * instead of something neat injected from the integration layer.
         */
        var requestCorrelationId = PlainSimpleRequestCorrelationId.create();

        var progressTrackerFactory  = new ProgressTrackerFactory(
            log,
            new LoggerForProgressTrackingAdapter(log),
            requestCorrelationId,
            taskRegistryFactory
        );

         return progressTrackerFactory.create(
             progressTask,
             configuration.jobId(),
             configuration.concurrency(),
             configuration.logProgress()
         );
    }

    ALGO build(
        G graphOrGraphStore,
        CONFIG configuration,
        ProgressTracker progressTracker,
        TerminationFlag terminationFlag
    );

    default Task progressTask(G graphOrGraphStore, CONFIG config, MemoryRange memoryEstimationInBytes) {
        return Tasks.leaf(taskName(), config.concurrency(), memoryEstimationInBytes);
    }

    /**
     * The name of the task. Typically, the name of the algorithm, but Java type params are not good enough.
     * Used for progress logging.
     *
     * @return the name of the task that logs progress
     */
    String taskName();

    /**
     * Returns an estimation about the memory consumption of that algorithm. The memory estimation can be used to
     * compute the actual consumption depending on {@link org.neo4j.gds.core.GraphDimensions} and concurrency.
     *
     * @return memory estimation
     * @see org.neo4j.gds.mem.MemoryEstimations
     * @see org.neo4j.gds.mem.MemoryEstimation#estimate(org.neo4j.gds.core.GraphDimensions, org.neo4j.gds.core.concurrency.Concurrency)
     */
    default MemoryEstimation memoryEstimation(CONFIG configuration) {
        throw new MemoryEstimationNotImplementedException();
    }

    Pair<ALGO, ProgressTracker> accept(Visitor<ALGO, CONFIG> visitor);

    interface Visitor<ALGO extends Algorithm<?>, CONFIG extends AlgoBaseConfig> {
        Pair<ALGO, ProgressTracker> graph(GraphAlgorithmFactory<ALGO, CONFIG> graphAlgorithmFactory);
    }
}
