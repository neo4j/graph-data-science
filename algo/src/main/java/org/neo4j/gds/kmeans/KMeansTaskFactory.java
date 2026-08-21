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
import org.neo4j.gds.api.nodes.IdMap;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel;
import org.neo4j.gds.progress.tasks.Task;
import org.neo4j.gds.progress.tasks.Tasks;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

public final class KMeansTaskFactory {
    private KMeansTaskFactory() {}

    public static Task createTask(Graph graph, KmeansParameters parameters) {
        var label = AlgorithmLabel.KMeans.asString();

        var iterations = parameters.numberOfRestarts();

        if (iterations == 1) {
            return kMeansSolo(graph, label, parameters);
        } else {
            return kMeansWithRestarts(label,graph, parameters);
        }

    }

    private static Task silhouetteTask(IdMap graph, KmeansParameters parameters) {
        return Tasks.leaf("Silhouette", parameters.concurrency(), graph.nodeCount());
    }

    private static Supplier<ArrayList<Task>> baseKmeansTaskList(KmeansParameters parameters) {
        var tasksList = new ArrayList<Task>();
        tasksList.add(Tasks.leaf("Initialization", parameters.concurrency(), parameters.k()));
        tasksList.add(Tasks.iterativeDynamic(
            "Main",
            parameters.concurrency(),
            () -> List.of(Tasks.leaf("Iteration", parameters.concurrency())),
            parameters.maxIterations()
        ));
        return () -> tasksList;
    }
    private static Task kMeansSolo(IdMap idMap, String description, KmeansParameters parameters) {

        var tasksList = baseKmeansTaskList(parameters).get();
        if (parameters.computeSilhouette()) {
            tasksList.add(silhouetteTask(idMap, parameters));
        }
        return Tasks.task(
            description,
            parameters.concurrency(),
            tasksList
        );
    }

    private static Task kMeansWithRestarts(String label,IdMap idMap, KmeansParameters parameters) {

        var iterativeTask = Tasks.iterativeFixed(
            "K-Means Restarts",
            parameters.concurrency(),
            () -> List.of(Tasks.task("K-Means Iteration", parameters.concurrency(), baseKmeansTaskList(parameters).get())),
            parameters.numberOfRestarts()
        );

       return Tasks.task(
            label,
            parameters.concurrency(),
            parameters.computeSilhouette() ? List.of(iterativeTask, silhouetteTask(idMap, parameters)) : List.of(iterativeTask)
        );
    }
}

