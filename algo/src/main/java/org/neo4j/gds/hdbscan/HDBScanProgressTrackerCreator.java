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
package org.neo4j.gds.hdbscan;

import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;

import java.util.List;

public final class HDBScanProgressTrackerCreator {
    private HDBScanProgressTrackerCreator() {}

    static Task kdBuildingTask(String name, Concurrency concurrency, long nodeCount) {
        return Tasks.leaf(name, concurrency, nodeCount);
    }

    static Task hierarchyTask(String name, Concurrency concurrency, long nodeCount) {
        return Tasks.leaf(name, concurrency, nodeCount - 1);
    }

    static Task condenseTask(String name, Concurrency concurrency, long nodeCount) {
        return Tasks.leaf(name, concurrency, nodeCount - 1);
    }

    static Task labellingTask(String name, Concurrency concurrency, long nodeCount) {
        return Tasks.task(
            name,
            concurrency,
            List.of(
                Tasks.leaf("Stability calculation", concurrency, nodeCount - 1),
                Tasks.leaf("cluster selection", concurrency, nodeCount - 1),
                Tasks.leaf("labelling", concurrency, nodeCount + nodeCount - 1)
            )
        );
    }

    static Task boruvkaTask(String name, Concurrency concurrency, long nodeCount) {
        return Tasks.leaf(name, concurrency, nodeCount - 1);
    }

    public static Task hdbscanTask(String name, Concurrency concurrency, long nodeCount) {
        return Tasks.task(
            name,
            concurrency,
            List.of(
                kdBuildingTask("KD-Tree Construction", concurrency, nodeCount),
                Tasks.leaf("Nearest Neighbors Search", concurrency, nodeCount),
                boruvkaTask("MST Computation", concurrency, nodeCount),
                hierarchyTask("Dendrogram Creation", concurrency, nodeCount),
                condenseTask("Condensed Tree Creation ", concurrency, nodeCount),
                labellingTask("Node Labelling", concurrency, nodeCount)
            )
        );
    }
}
