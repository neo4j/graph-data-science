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
package org.neo4j.gds.similarity.filterednodesim;

import org.neo4j.gds.NullComputationResultConsumer;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.ExecutionMode;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.procedures.algorithms.configuration.NewConfigFunction;
import org.neo4j.gds.procedures.algorithms.similarity.SimilarityWriteResult;
import org.neo4j.gds.similarity.nodesim.NodeSimilarity;
import org.neo4j.gds.similarity.nodesim.NodeSimilarityResult;

import java.util.stream.Stream;

import static org.neo4j.gds.similarity.filterednodesim.FilteredNodeSimilarityStreamProc.DESCRIPTION;

@GdsCallable(
    name = "gds.nodeSimilarity.filtered.write",
    aliases = {"gds.alpha.nodeSimilarity.filtered.write"},
    description = DESCRIPTION,
    executionMode = ExecutionMode.WRITE_RELATIONSHIP)
public class FilteredNodeSimilarityWriteSpec implements
    AlgorithmSpec<NodeSimilarity, NodeSimilarityResult, FilteredNodeSimilarityWriteConfig, Stream<SimilarityWriteResult>, FilteredNodeSimilarityFactory<FilteredNodeSimilarityWriteConfig>> {

    @Override
    public String name() {
        return "FilteredNodeSimilarityWrite";
    }

    @Override
    public FilteredNodeSimilarityFactory<FilteredNodeSimilarityWriteConfig> algorithmFactory(
        ExecutionContext executionContext
    ) {
        return new FilteredNodeSimilarityFactory<>();
    }

    @Override
    public NewConfigFunction<FilteredNodeSimilarityWriteConfig> newConfigFunction() {
        return (__, userInput) -> FilteredNodeSimilarityWriteConfig.of(userInput);
    }

    @Override
    public ComputationResultConsumer<NodeSimilarity, NodeSimilarityResult, FilteredNodeSimilarityWriteConfig, Stream<SimilarityWriteResult>> computationResultConsumer() {
        return new NullComputationResultConsumer<>();
    }
}
