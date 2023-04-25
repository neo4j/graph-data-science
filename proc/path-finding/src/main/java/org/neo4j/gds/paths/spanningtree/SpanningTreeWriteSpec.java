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

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.AlgorithmSpecProgressTrackerProvider;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;
import org.neo4j.gds.spanningtree.Prim;
import org.neo4j.gds.spanningtree.SpanningGraph;
import org.neo4j.gds.spanningtree.SpanningTree;
import org.neo4j.gds.spanningtree.SpanningTreeAlgorithmFactory;
import org.neo4j.gds.spanningtree.SpanningTreeWriteConfig;

import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.MUTATE_RELATIONSHIP;

@GdsCallable(name = "gds.beta.spanningTree.write", description = SpanningTreeWriteProc.DESCRIPTION, executionMode = MUTATE_RELATIONSHIP)
public class SpanningTreeWriteSpec implements AlgorithmSpec<Prim, SpanningTree, SpanningTreeWriteConfig, Stream<WriteResult>, SpanningTreeAlgorithmFactory<SpanningTreeWriteConfig>> {

    @Override
    public String name() {
        return "SpanningTreeWrite";
    }

    @Override
    public SpanningTreeAlgorithmFactory<SpanningTreeWriteConfig> algorithmFactory() {
        return new SpanningTreeAlgorithmFactory<>();
    }

    @Override
    public NewConfigFunction<SpanningTreeWriteConfig> newConfigFunction() {
        return (__, config) -> SpanningTreeWriteConfig.of(config);

    }

    public ComputationResultConsumer<Prim, SpanningTree, SpanningTreeWriteConfig, Stream<WriteResult>> computationResultConsumer() {

        return (computationResult, executionContext) -> {
            WriteResult.Builder builder = new WriteResult.Builder();

            if (computationResult.result().isEmpty()) {
                return Stream.of(builder.build());
            }

            Graph graph = computationResult.graph();
            Prim prim = computationResult.algorithm();
            SpanningTree spanningTree = computationResult.result().get();
            SpanningTreeWriteConfig config = computationResult.config();

            builder
                .withEffectiveNodeCount(spanningTree.effectiveNodeCount())
                .withTotalWeight(spanningTree.totalWeight());

            try (ProgressTimer ignored = ProgressTimer.start(builder::withWriteMillis)) {

                var spanningGraph = new SpanningGraph(graph, spanningTree);

                executionContext.relationshipExporterBuilder()
                    .withGraph(spanningGraph)
                    .withIdMappingOperator(spanningGraph::toOriginalNodeId)
                    .withTerminationFlag(prim.getTerminationFlag())
                    .withProgressTracker(AlgorithmSpecProgressTrackerProvider.createProgressTracker(
                        name(),
                        graph.nodeCount(),
                        config.writeConcurrency(),
                        executionContext
                    ))
                    .withArrowConnectionInfo(config.arrowConnectionInfo())
                    .build()
                    .write(config.writeRelationshipType(), config.writeProperty());
            }
            builder.withComputeMillis(computationResult.computeMillis());
            builder.withPreProcessingMillis(computationResult.preProcessingMillis());
            builder.withRelationshipsWritten(spanningTree.effectiveNodeCount() - 1);
            builder.withConfig(config);
            return Stream.of(builder.build());
        };
    }
}
