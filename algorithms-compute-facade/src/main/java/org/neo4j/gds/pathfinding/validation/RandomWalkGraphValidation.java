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
package org.neo4j.gds.pathfinding.validation;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.loading.validation.GraphValidation;
import org.neo4j.gds.core.utils.partition.PartitionUtils;

import java.util.Optional;
import java.util.concurrent.ExecutorService;

public class RandomWalkGraphValidation implements GraphValidation {

    private final Concurrency concurrency;
    private final ExecutorService executorService;

    public RandomWalkGraphValidation(Concurrency concurrency, ExecutorService executorService) {
        this.concurrency = concurrency;
        this.executorService = executorService;
    }

    @Override
    public void validate(Graph graph) {
        if (graph.hasRelationshipProperty()) {
            var tasks = PartitionUtils.degreePartition(
                graph,
                concurrency,
                partition -> new RelationshipValidator(
                    graph.concurrentCopy(), partition, weight -> weight >= 0,
                    "RandomWalk only supports non-negative weights."
                ),
                Optional.empty()
            );

            RunWithConcurrency.builder()
                .concurrency(concurrency)
                .tasks(tasks)
                .executor(executorService)
                .run();
        }

    }
}
