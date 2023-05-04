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
package org.neo4j.gds.pagerank;

import org.neo4j.gds.common.CentralityStreamResult;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;

import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.STREAM;
import static org.neo4j.gds.pagerank.PageRankProcCompanion.PAGE_RANK_DESCRIPTION;

@GdsCallable(name = "gds.pageRank.stream", description = PAGE_RANK_DESCRIPTION, executionMode = STREAM)
public class PageRankStreamSpec implements AlgorithmSpec<PageRankAlgorithm, PageRankResult,PageRankStreamConfig,Stream<CentralityStreamResult>,PageRankAlgorithmFactory<PageRankStreamConfig>> {

    @Override
    public String name() {
        return "PageRankStream";
    }

    @Override
    public PageRankAlgorithmFactory<PageRankStreamConfig> algorithmFactory(ExecutionContext executionContext) {
        return new PageRankAlgorithmFactory<>();
    }

    @Override
    public NewConfigFunction<PageRankStreamConfig> newConfigFunction() {
        return (___,config) -> PageRankStreamConfig.of(config);
    }

    @Override
    public ComputationResultConsumer<PageRankAlgorithm, PageRankResult, PageRankStreamConfig, Stream<CentralityStreamResult>> computationResultConsumer() {
        return (computationResult, executionContext) -> {
            if (computationResult.result().isEmpty()) {
                return Stream.empty();
            }
            var graph = computationResult.graph();
            var scores = computationResult.result().get().scores();
            return LongStream.range(0, graph.nodeCount()).mapToObj(nodeId -> {
                return new CentralityStreamResult(
                    graph.toOriginalNodeId(nodeId),
                    scores.get(nodeId)
                );
            });
        };

    }
}
