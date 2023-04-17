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
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;
import org.neo4j.gds.spanningtree.Prim;
import org.neo4j.gds.spanningtree.SpanningTree;
import org.neo4j.gds.spanningtree.SpanningTreeAlgorithmFactory;
import org.neo4j.gds.spanningtree.SpanningTreeStatsConfig;

import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.STATS;

@GdsCallable(name = "gds.beta.spanningTree.stats", description = SpanningTreeWriteProc.DESCRIPTION, executionMode = STATS)
public class SpanningTreeStatsSpec implements AlgorithmSpec<Prim, SpanningTree, SpanningTreeStatsConfig, Stream<StatsResult>, SpanningTreeAlgorithmFactory<SpanningTreeStatsConfig>> {

    @Override
    public String name() {
        return "SpanningTreeStats";
    }

    @Override
    public SpanningTreeAlgorithmFactory<SpanningTreeStatsConfig> algorithmFactory() {
        return new SpanningTreeAlgorithmFactory<>();
    }

    @Override
    public NewConfigFunction<SpanningTreeStatsConfig> newConfigFunction() {
        return (__, config) -> SpanningTreeStatsConfig.of(config);

    }

    public ComputationResultConsumer<Prim, SpanningTree, SpanningTreeStatsConfig, Stream<StatsResult>> computationResultConsumer() {

        return (computationResult, executionContext) -> {

            StatsResult.Builder builder = new StatsResult.Builder();

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
