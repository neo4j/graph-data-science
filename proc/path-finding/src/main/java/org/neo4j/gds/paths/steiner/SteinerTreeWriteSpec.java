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
package org.neo4j.gds.paths.steiner;

import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.write.RelationshipExporterBuilder;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;
import org.neo4j.gds.spanningtree.SpanningGraph;
import org.neo4j.gds.spanningtree.SpanningTree;
import org.neo4j.gds.steiner.ShortestPathsSteinerAlgorithm;
import org.neo4j.gds.steiner.SteinerTreeAlgorithmFactory;
import org.neo4j.gds.steiner.SteinerTreeResult;
import org.neo4j.gds.steiner.SteinerTreeWriteConfig;

import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.WRITE_RELATIONSHIP;

@GdsCallable(name = "gds.beta.SteinerTree.write", description = SteinerTreeStatsProc.DESCRIPTION, executionMode = WRITE_RELATIONSHIP)
public class SteinerTreeWriteSpec implements AlgorithmSpec<ShortestPathsSteinerAlgorithm, SteinerTreeResult, SteinerTreeWriteConfig, Stream<WriteResult>, SteinerTreeAlgorithmFactory<SteinerTreeWriteConfig>> {

    @Override
    public String name() {
        return "SteinerTreeWrite";
    }

    @Override
    public SteinerTreeAlgorithmFactory<SteinerTreeWriteConfig> algorithmFactory() {
        return new SteinerTreeAlgorithmFactory<>();
    }

    @Override
    public NewConfigFunction<SteinerTreeWriteConfig> newConfigFunction() {
        return (__, config) -> SteinerTreeWriteConfig.of(config);
    }

    public ComputationResultConsumer<ShortestPathsSteinerAlgorithm, SteinerTreeResult, SteinerTreeWriteConfig, Stream<WriteResult>> computationResultConsumer() {

        return (computationResult, executionContext) -> {
            var config = computationResult.config();
            var terminationFlag = computationResult.algorithm().getTerminationFlag();
            var sourceNode = config.sourceNode();
            var graph = computationResult.graph();

            var builder = new WriteResult.Builder();

            computationResult.result().ifPresent(steinerTreeResult -> {
                builder
                    .withEffectiveNodeCount(steinerTreeResult.effectiveNodeCount())
                    .withEffectiveTargetNodeCount(steinerTreeResult.effectiveTargetNodesCount())
                    .withTotalWeight(steinerTreeResult.totalCost());

                try (ProgressTimer ignored = ProgressTimer.start(builder::withWriteMillis)) {
                    var relationshipToParentCost = steinerTreeResult.relationshipToParentCost();
                    var spanningTree = new SpanningTree(
                        graph.toMappedNodeId(sourceNode),
                        graph.nodeCount(),
                        steinerTreeResult.effectiveNodeCount(),
                        steinerTreeResult.parentArray(),
                        nodeId -> relationshipToParentCost.get(nodeId),
                        steinerTreeResult.totalCost()
                    );
                    var spanningGraph = new SpanningGraph(graph, spanningTree);

                    RelationshipExporterBuilder relationshipExporterBuilder = executionContext.relationshipExporterBuilder();
                    relationshipExporterBuilder
                        .withGraph(spanningGraph)
                        .withIdMappingOperator(spanningGraph::toOriginalNodeId)
                        .withTerminationFlag(terminationFlag)
                        .withProgressTracker(ProgressTracker.NULL_TRACKER)
                        .withArrowConnectionInfo(config.arrowConnectionInfo())
                        .build()
                        .write(
                            config.writeRelationshipType(),
                            config.writeProperty()
                        );

                }
                builder.withRelationshipsWritten(steinerTreeResult.effectiveNodeCount() - 1);
            });

            return Stream.of(builder
                .withComputeMillis(computationResult.computeMillis())
                .withPreProcessingMillis(computationResult.preProcessingMillis())
                .withConfig(config)
                .build());
        };
    }
}
