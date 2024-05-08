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
package org.neo4j.gds.applications.algorithms.similarity;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.similarity.SimilarityGraphBuilder;
import org.neo4j.gds.similarity.SimilarityGraphResult;
import org.neo4j.gds.similarity.SimilarityResult;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.stream.Stream;

/**
 * "Delegate" because really we should mint a microtype and place this behaviour in it
 */
public class SimilarityResultStreamDelegate {
    public SimilarityGraphResult computeSimilarityGraph(
        Graph graph,
        Concurrency concurrency,
        Stream<SimilarityResult> similarityResultStream
    ) {
        var similarityGraph = new SimilarityGraphBuilder(
            graph,
            concurrency,
            DefaultPool.INSTANCE,
            TerminationFlag.RUNNING_TRUE
        ).build(similarityResultStream);

        return new SimilarityGraphResult(similarityGraph, graph.nodeCount(), false);
    }
}
