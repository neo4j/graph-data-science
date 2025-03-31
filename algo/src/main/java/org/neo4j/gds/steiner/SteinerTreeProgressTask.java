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
package org.neo4j.gds.steiner;

import org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;

import java.util.ArrayList;

public final class SteinerTreeProgressTask {

    private SteinerTreeProgressTask() {}

    public static Task create(SteinerTreeParameters parameters, long nodeCount) {
        var subtasks = new ArrayList<Task>();
        subtasks.add(Tasks.leaf("Traverse", parameters.targetNodes().size()));
        if (parameters.applyRerouting()) {
            subtasks.add(Tasks.leaf("Reroute", nodeCount));
        }
        return Tasks.task(AlgorithmLabel.SteinerTree.asString(), subtasks);

    }
}
