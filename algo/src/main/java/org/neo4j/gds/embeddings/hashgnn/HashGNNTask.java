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
package org.neo4j.gds.embeddings.hashgnn;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;

import java.util.ArrayList;
import java.util.List;

public class HashGNNTask {
    public static Task create(Graph graph, HashGNNConfig config) {
        var tasks = new ArrayList<Task>();

        if (config.generateFeatures().isPresent()) {
            tasks.add(Tasks.leaf("Generate base node property features", graph.nodeCount()));
        } else if (config.binarizeFeatures().isPresent()) {
            tasks.add(Tasks.leaf("Binarize node property features", graph.nodeCount()));
        } else {
            tasks.add(Tasks.leaf("Extract raw node property features", graph.nodeCount()));
        }

        int numRelTypes = config.heterogeneous() ? config.relationshipTypes().size() : 1;

        tasks.add(Tasks.iterativeFixed(
            "Propagate embeddings",
            () -> List.of(
                Tasks.leaf(
                    "Precompute hashes",
                    config.embeddingDensity() * (1 + 1 + numRelTypes)
                ),
                Tasks.leaf(
                    "Perform min-hashing",
                    (2 * graph.nodeCount() + graph.relationshipCount()) * config.embeddingDensity()
                )
            ),
            config.iterations()
        ));

        if (config.outputDimension().isPresent()) {
            tasks.add(Tasks.leaf("Densify output embeddings", graph.nodeCount()));
        }

        return Tasks.task(
            AlgorithmLabel.HashGNN.asString(),
            tasks
        );
    }
}