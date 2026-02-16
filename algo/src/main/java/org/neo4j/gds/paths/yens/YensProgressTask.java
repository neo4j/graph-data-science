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
package org.neo4j.gds.paths.yens;

import org.neo4j.gds.api.GraphCharacteristics;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.paths.delta.DeltaSteppingProgressTask;

public final class YensProgressTask {

    private YensProgressTask() {}

    public static Task createBasicYens(long relationshipCount, int k) {
        var initialTask = Tasks.leaf(AlgorithmLabel.Dijkstra.asString(), relationshipCount);
        var pathGrowingTask = Tasks.leaf("Path growing", k - 1);
        return Tasks.task(AlgorithmLabel.Yens.asString(), initialTask, pathGrowingTask);
    }

    public static Task createYensWithPruning(long nodeCount, long relationshipCount, int k) {
        var deltaForward = DeltaSteppingProgressTask.create("Shortest path from source");
        var deltaBack = DeltaSteppingProgressTask.create("Shortest path to target");
        var filterNodes = Tasks.leaf("Filter nodes", nodeCount);
        var graphCreate = Tasks.leaf("Create pruned graph", relationshipCount);
        var yensTask = createBasicYens(relationshipCount, k);
        var pruningTask = Tasks.task("Peek pruning", deltaForward, deltaBack, filterNodes, graphCreate);
        return Tasks.task(AlgorithmLabel.Yens.asString(), pruningTask, yensTask);
    }

    public static Task create(GraphCharacteristics characteristics, long nodeCount, long relationshipCount, int k) {
        return YensFactory.isPeekPruningAppropriate(characteristics) ?
            createYensWithPruning(nodeCount, relationshipCount, k) :
            createBasicYens(relationshipCount, k);
    }
}
