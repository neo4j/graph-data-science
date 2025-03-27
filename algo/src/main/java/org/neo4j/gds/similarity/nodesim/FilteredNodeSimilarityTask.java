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
package org.neo4j.gds.similarity.nodesim;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.wcc.WccTask;

import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.FilteredNodeSimilarity;

public class FilteredNodeSimilarityTask {

    private FilteredNodeSimilarityTask() {}

    public static Task create(Graph graph, NodeSimilarityParameters parameters) {
        return  Tasks.task(
            FilteredNodeSimilarity.asString(),
            filteredNodeSimilarityProgressTask(graph, parameters.runWCC()),
            Tasks.leaf("compare node pairs")
        );
    }

    private static Task filteredNodeSimilarityProgressTask(Graph graph, boolean runWcc) {
        if (runWcc) {
            return Tasks.task(
                "prepare",
                WccTask.create(graph),
                Tasks.leaf("initialize", graph.relationshipCount())
            );
        }
        return Tasks.leaf("prepare", graph.relationshipCount());
    }
}
