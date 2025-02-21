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

import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;

import java.util.List;

public class HDBScanProgressTrackerCreator {

    static Task kdBuildingTask(String name, long nodeCount){
        return Tasks.leaf(name, nodeCount);
    }

    static Task hierarchyTask(String name, long nodeCount){
        return Tasks.leaf(name,nodeCount - 1);
    }

    static Task condenseTask(String name, long nodeCount){
        return Tasks.leaf(name,nodeCount - 1);
    }

    static Task labellingTask(String name, long nodeCount){
        return Tasks.task(
                name,
                List.of(
                    Tasks.leaf("Stability calculation", nodeCount-1),
                    Tasks.leaf("cluster selection", nodeCount-1),
                    Tasks.leaf("labelling", nodeCount + nodeCount-1)
                )
        );
    }

    static Task boruvkaTask(String name, long nodeCount){
        return Tasks.leaf(name,nodeCount - 1);
    }

    static Task hdbscanTask(String name, long nodeCount){
        return Tasks.task(
            name,
            List.of(
                kdBuildingTask("KD-Tree Construction",nodeCount),
                Tasks.leaf("Nearest Neighbors Search", nodeCount),
                boruvkaTask("MST Computation", nodeCount),
                hierarchyTask("Dendrogram Creation", nodeCount),
                condenseTask("Condensed Tree Creation ", nodeCount),
                labellingTask("Node Labelling", nodeCount)
            )
        );

    }

}
