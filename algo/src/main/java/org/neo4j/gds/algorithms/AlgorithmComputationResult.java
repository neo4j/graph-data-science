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
package org.neo4j.gds.algorithms;

import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.ResultStore;

import java.util.Optional;

@ValueClass
public interface AlgorithmComputationResult<RESULT> {

    /**
     * Result is empty if no computation happened, which basically means the graph was empty.
     * @return Optional.empty() if no computation happened, for example, when the graph was empty,
     *         otherwise Optional containing the result of the algorithm execution.
     */
    Optional<RESULT> result();

    Graph graph();

    GraphStore graphStore();

    ResultStore resultStore();

    static <R> AlgorithmComputationResult<R> of(
        R result,
        Graph graph,
        GraphStore graphStore,
        ResultStore resultStore
    ) {
        return ImmutableAlgorithmComputationResult.of(
            Optional.of(result),
            graph,
            graphStore,
            resultStore
        );
    }

    static <R> AlgorithmComputationResult<R> withoutAlgorithmResult(Graph graph, GraphStore graphStore, ResultStore resultStore) {
        return ImmutableAlgorithmComputationResult.of(
            Optional.empty(),
            graph,
            graphStore,
            resultStore
        );
    }
}
