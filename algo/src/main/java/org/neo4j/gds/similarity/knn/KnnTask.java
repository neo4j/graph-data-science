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
package org.neo4j.gds.similarity.knn;

import org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;

import java.util.List;

public final class KnnTask {
    private KnnTask() {}

    public static Task create(long nodeCount, int maxIterations) {
        return Tasks.task(
            AlgorithmLabel.KNN.asString(),
            Tasks.leaf("Initialize random neighbors", nodeCount),
            Tasks.iterativeDynamic(
                "Iteration",
                () -> List.of(
                    Tasks.leaf("Split old and new neighbors", nodeCount),
                    Tasks.leaf("Reverse old and new neighbors", nodeCount),
                    Tasks.leaf("Join neighbors", nodeCount)
                ),
                maxIterations
            )
        );
    }
}
