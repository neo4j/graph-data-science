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
package org.neo4j.gds.modularityoptimization;

import org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.k1coloring.K1ColoringProgressTrackerTaskCreator;

import java.util.List;

public final class ModularityOptimizationProgressTrackerTaskCreator {

    private ModularityOptimizationProgressTrackerTaskCreator() {}

    public static Task progressTask(long nodeCount, long relationshipCount, int maxIterations) {

        var coloringTask = K1ColoringProgressTrackerTaskCreator.progressTask(
            nodeCount,
            ModularityOptimization.K1COLORING_MAX_ITERATIONS
        );
        return Tasks.task(
            AlgorithmLabel.ModularityOptimization.asString(),
            Tasks.task(
                "initialization",
                coloringTask
            ),
            Tasks.iterativeDynamic(
                "compute modularity",
                () -> List.of(Tasks.leaf("optimizeForColor", relationshipCount)),
                maxIterations
            )
        );
    }
}
