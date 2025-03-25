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
package org.neo4j.gds.kmeans;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;

import java.util.List;

public final class KMeansTaskFactory {

    private KMeansTaskFactory() {}

    public static Task createTask(Graph graph, KmeansParameters parameters) {
        var label = AlgorithmLabel.KMeans.asString();

        var iterations = parameters.numberOfRestarts();
        if (iterations == 1) {
            return kMeansTask(graph, label, parameters);
        }

        return Tasks.iterativeFixed(
            label,
            () -> List.of(kMeansTask(graph, "KMeans Iteration", parameters)),
            iterations
        );
    }

    private static Task kMeansTask(IdMap idMap, String description, KmeansParameters parameters) {
        if (parameters.computeSilhouette()) {
            return Tasks.task(
                description, List.of(
                    Tasks.leaf("Initialization", parameters.k()),
                    Tasks.iterativeDynamic(
                        "Main",
                        () -> List.of(Tasks.leaf("Iteration")),
                        parameters.maxIterations()
                    ),
                    Tasks.leaf("Silhouette", idMap.nodeCount())

                )
            );
        } else {
            return Tasks.task(
                description, List.of(
                    Tasks.leaf("Initialization", parameters.k()),
                    Tasks.iterativeDynamic(
                        "Main",
                        () -> List.of(Tasks.leaf("Iteration")),
                        parameters.maxIterations()
                    )
                )
            );
        }
    }
}
