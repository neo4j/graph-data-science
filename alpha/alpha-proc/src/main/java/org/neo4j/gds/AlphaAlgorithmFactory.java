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

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.core.utils.BatchingProgressLogger;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.progress.ProgressEventTracker;
import org.neo4j.gds.core.utils.progress.v2.tasks.TaskProgressTracker;
import org.neo4j.gds.exceptions.MemoryEstimationNotImplementedException;
import org.neo4j.logging.Log;

@FunctionalInterface
public interface AlphaAlgorithmFactory<ALGO extends Algorithm<ALGO, ?>, CONFIG extends AlgoBaseConfig> extends AlgorithmFactory<ALGO, CONFIG> {
    @Override
    default ALGO build(
        Graph graph,
        CONFIG configuration,
        AllocationTracker tracker,
        Log log,
        ProgressEventTracker eventTracker
    ) {
        ALGO algo = buildAlphaAlgo(graph, configuration, tracker, log, eventTracker);
        var progressTask = progressTask(graph, configuration);
        var progressLogger = new BatchingProgressLogger(log, progressTask, configuration.concurrency());
        var progressTracker = new TaskProgressTracker(progressTask, progressLogger, eventTracker);
        return algo.withProgressTracker(progressTracker);
    }

    ALGO buildAlphaAlgo(
        Graph graph,
        CONFIG configuration,
        AllocationTracker tracker,
        Log log,
        ProgressEventTracker eventTracker
    );

    @Override
    default MemoryEstimation memoryEstimation(CONFIG configuration) {
        throw new MemoryEstimationNotImplementedException();
    }
}
