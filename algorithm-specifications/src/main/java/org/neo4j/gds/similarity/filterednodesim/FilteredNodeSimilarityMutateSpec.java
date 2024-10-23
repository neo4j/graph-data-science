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
import org.neo4j.gds.procedures.algorithms.similarity.SimilarityMutateResult;
import org.neo4j.gds.similarity.nodesim.NodeSimilarity;
import org.neo4j.gds.similarity.nodesim.NodeSimilarityResult;

import java.util.stream.Stream;

import static org.neo4j.gds.similarity.filterednodesim.Constants.FILTERED_NODE_SIMILARITY_DESCRIPTION;

@GdsCallable(name = "gds.nodeSimilarity.filtered.mutate", aliases = {"gds.alpha.nodeSimilarity.filtered.mutate"}, description = FILTERED_NODE_SIMILARITY_DESCRIPTION, executionMode = ExecutionMode.MUTATE_RELATIONSHIP)
public class FilteredNodeSimilarityMutateSpec  implements AlgorithmSpec<
    NodeSimilarity,
    NodeSimilarityResult,
    FilteredNodeSimilarityMutateConfig,
    Stream<SimilarityMutateResult>,
    FilteredNodeSimilarityFactory<FilteredNodeSimilarityMutateConfig>
    > {

    @Override
    public String name() {
        return "FilteredNodeSimilarityMutate";
    }


    @Override
    public FilteredNodeSimilarityFactory<FilteredNodeSimilarityMutateConfig> algorithmFactory(ExecutionContext executionContext) {
        return new FilteredNodeSimilarityFactory<>();
    }

    @Override
    public NewConfigFunction<FilteredNodeSimilarityMutateConfig> newConfigFunction() {
        return (__, config) -> FilteredNodeSimilarityMutateConfig.of(config);
    }

    @Override
    public ComputationResultConsumer<NodeSimilarity, NodeSimilarityResult, FilteredNodeSimilarityMutateConfig, Stream<SimilarityMutateResult>> computationResultConsumer() {
        return new NullComputationResultConsumer<>();
    }
}
