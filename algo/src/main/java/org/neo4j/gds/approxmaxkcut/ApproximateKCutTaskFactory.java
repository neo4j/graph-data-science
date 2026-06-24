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
package org.neo4j.gds.approxmaxkcut;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.progress.tasks.Task;
import org.neo4j.gds.progress.tasks.Tasks;

import java.util.List;

public final class ApproximateKCutTaskFactory {

    private ApproximateKCutTaskFactory() {}

    public static Task createTask(Graph graph, ApproxMaxKCutParameters parameters) {
        return Tasks.iterativeFixed(
            AlgorithmLabel.ApproximateMaximumKCut.asString(),
            parameters.concurrency(),
            () -> List.of(
                Tasks.leaf("place nodes randomly", parameters.concurrency(), graph.nodeCount()),
                searchTask(parameters.concurrency(), graph.nodeCount(), parameters.vnsMaxNeighborhoodOrder())
            ),
            parameters.iterations()
        );
    }

    private static Task searchTask(Concurrency concurrency, long nodeCount, int vnsMaxNeighborhoodOrder) {
        if (vnsMaxNeighborhoodOrder > 0) {
            return Tasks.iterativeOpen(
                "variable neighborhood search",
                concurrency,
                () -> List.of(localSearchTask(concurrency, nodeCount))
            );
        }

        return localSearchTask(concurrency, nodeCount);
    }

    private static Task localSearchTask(Concurrency concurrency, long nodeCount) {
        return Tasks.task(
            "local search", concurrency, Tasks.iterativeOpen(
                "improvement loop",
                concurrency,
                () -> List.of(
                    Tasks.leaf("compute node to community weights", concurrency, nodeCount),
                    Tasks.leaf("swap for local improvements", concurrency, nodeCount)
                )
            ), Tasks.leaf("compute current solution cost", concurrency, nodeCount)
        );
    }


}
