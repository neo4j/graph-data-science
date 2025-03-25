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
package org.neo4j.gds.embeddings.node2vec;

import node2vec.Node2VecParameters;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.degree.DegreeCentralityTask;

import java.util.ArrayList;
import java.util.List;

public class Node2VecTask {

    private Node2VecTask() {}

    public static Task create(Graph graph, Node2VecParameters parameters) {
        var randomWalkTasks = new ArrayList<Task>();
        if (graph.hasRelationshipProperty()) {
            randomWalkTasks.add(DegreeCentralityTask.create(graph));
        }
        randomWalkTasks.add(Tasks.leaf("create walks", graph.nodeCount()));
        return Tasks.task(
            AlgorithmLabel.Node2Vec.asString(),
            Tasks.task("RandomWalk", randomWalkTasks),
            Tasks.iterativeFixed(
                "train",
                () -> List.of(Tasks.leaf("iteration")),
                parameters.trainParameters().iterations()
            )
        );
    }
}
