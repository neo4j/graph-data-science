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

import org.jetbrains.annotations.NotNull;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.TaskProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.TaskTreeProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.core.utils.warnings.EmptyUserLogRegistryFactory;
import org.neo4j.gds.core.utils.warnings.UserLogRegistryFactory;
import org.neo4j.gds.exceptions.MemoryEstimationNotImplementedException;
import org.neo4j.logging.Log;

public interface AlgorithmFactory<G, ALGO extends Algorithm<?>, CONFIG extends AlgoBaseConfig> {
    default ALGO build(
        G graphOrGraphStore,
        CONFIG configuration,
        Log log,
        TaskRegistryFactory taskRegistryFactory
    ) {
        return this.build(
            graphOrGraphStore,
            configuration,
            log,
            taskRegistryFactory,
            EmptyUserLogRegistryFactory.INSTANCE
        );
    }

    default ALGO build(
        G graphOrGraphStore,
        CONFIG configuration,
        Log log,
        TaskRegistryFactory taskRegistryFactory,
        UserLogRegistryFactory userLogRegistryFactory
    ) {
        var progressTracker = createProgressTracker(
            configuration,
            log,
            taskRegistryFactory,
            userLogRegistryFactory,
            progressTask(graphOrGraphStore, configuration)
        );
        return build(graphOrGraphStore, configuration, progressTracker);
    }

    @NotNull
    private ProgressTracker createProgressTracker(
        CONFIG configuration,
        Log log,
        TaskRegistryFactory taskRegistryFactory,
        UserLogRegistryFactory userLogRegistryFactory,
        Task progressTask
    ) {
        ProgressTracker progressTracker;
        if (configuration.logProgress()) {
            progressTracker = new TaskProgressTracker(
                progressTask,
                log,
                configuration.concurrency(),
                configuration.jobId(),
                taskRegistryFactory,
                userLogRegistryFactory
            );
        } else {
            progressTracker = new TaskTreeProgressTracker(
                progressTask,
                log,
                configuration.concurrency(),
                configuration.jobId(),
                taskRegistryFactory,
                userLogRegistryFactory
            );
        }
        return progressTracker;
    }

    ALGO build(
        G graphOrGraphStore,
        CONFIG configuration,
        ProgressTracker progressTracker
    );

    default Task progressTask(G graphOrGraphStore, CONFIG config) {
        return Tasks.leaf(taskName());
    }

    /**
     * The name of the task. Typically the name of the algorithm, but Java type params are not good enough.
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
     * @see org.neo4j.gds.core.utils.mem.MemoryEstimations
     * @see org.neo4j.gds.core.utils.mem.MemoryEstimation#estimate(org.neo4j.gds.core.GraphDimensions, int)
     */
    default MemoryEstimation memoryEstimation(CONFIG configuration) {
        throw new MemoryEstimationNotImplementedException();
    }

    /**
     * For memory estimation, this allows us to inject expected counts for relationship types and labels.
     * These types and labels would be created during the algorithm execution.
     */
    default GraphDimensions estimatedGraphDimensionTransformer(GraphDimensions graphDimensions, CONFIG config) {
        return graphDimensions;
    }

    ALGO accept(Visitor<ALGO, CONFIG> visitor);

    interface Visitor<ALGO extends Algorithm<?>, CONFIG extends AlgoBaseConfig> {
        ALGO graph(GraphAlgorithmFactory<ALGO, CONFIG> graphAlgorithmFactory);
        ALGO graphStore(GraphStoreAlgorithmFactory<ALGO, CONFIG> graphStoreAlgorithmFactory);
    }
}
