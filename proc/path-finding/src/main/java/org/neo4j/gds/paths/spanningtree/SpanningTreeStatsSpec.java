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
package org.neo4j.gds.paths.spanningtree;

import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.procedures.algorithms.configuration.NewConfigFunction;
import org.neo4j.gds.procedures.algorithms.pathfinding.SpanningTreeStatsResult;
import org.neo4j.gds.spanningtree.Prim;
import org.neo4j.gds.spanningtree.SpanningTree;
import org.neo4j.gds.spanningtree.SpanningTreeAlgorithmFactory;
import org.neo4j.gds.spanningtree.SpanningTreeStatsConfig;

import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.STATS;

@GdsCallable(
    name = "gds.spanningTree.stats",
    aliases = {"gds.beta.spanningTree.stats"},
    description = Constants.SPANNING_TREE_DESCRIPTION,
    executionMode = STATS
)
public class SpanningTreeStatsSpec implements AlgorithmSpec<Prim, SpanningTree, SpanningTreeStatsConfig, Stream<SpanningTreeStatsResult>, SpanningTreeAlgorithmFactory<SpanningTreeStatsConfig>> {

    @Override
    public String name() {
        return "SpanningTreeStats";
    }

    @Override
    public SpanningTreeAlgorithmFactory<SpanningTreeStatsConfig> algorithmFactory(ExecutionContext executionContext) {
        return new SpanningTreeAlgorithmFactory<>();
    }

    @Override
    public NewConfigFunction<SpanningTreeStatsConfig> newConfigFunction() {
        return (__, config) -> SpanningTreeStatsConfig.of(config);

    }

    public ComputationResultConsumer<Prim, SpanningTree, SpanningTreeStatsConfig, Stream<SpanningTreeStatsResult>> computationResultConsumer() {

        return (computationResult, executionContext) -> {

            SpanningTreeStatsResult.Builder builder = new SpanningTreeStatsResult.Builder();

            if (computationResult.result().isEmpty()) {
                return Stream.of(builder.build());
            }

            SpanningTree spanningTree = computationResult.result().get();
            SpanningTreeStatsConfig config = computationResult.config();

            builder.withEffectiveNodeCount(spanningTree.effectiveNodeCount());
            builder.withTotalWeight(spanningTree.totalWeight());

            builder.withComputeMillis(computationResult.computeMillis());
            builder.withPreProcessingMillis(computationResult.preProcessingMillis());
            builder.withConfig(config);
            return Stream.of(builder.build());
        };
    }
}
