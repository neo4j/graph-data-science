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
package org.neo4j.gds.hits;

import org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.indexInverse.InverseRelationshipsTask;
import org.neo4j.gds.indexinverse.InverseRelationshipsParameters;

import java.util.List;

public final class HitsProgressTrackerCreator {

    private HitsProgressTrackerCreator() {}

    public static Task progressTask(Concurrency concurrency, long nodeCount, int maxIterations) {
        return Tasks.iterativeDynamic(
            AlgorithmLabel.HITS.asString(),
            concurrency, () -> List.of(
                Tasks.leaf("Compute iteration", concurrency, nodeCount),
                Tasks.leaf("Master compute iteration", concurrency, nodeCount)
            ),
            maxIterations
        );
    }

    public static Task progressTaskWithInvertedIndex(
        long nodeCount,
        int maxIterations,
        InverseRelationshipsParameters inverseParams
    ) {
        var hitsTask = progressTask(inverseParams.concurrency(), nodeCount, maxIterations);
        var invTask = InverseRelationshipsTask.progressTask(inverseParams.concurrency(), nodeCount, inverseParams);
        return Tasks.task(AlgorithmLabel.HITS.asString(), inverseParams.concurrency(), invTask, hitsTask);

    }
}
