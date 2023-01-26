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

import org.neo4j.gds.Orientation;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.loading.SingleTypeRelationships;
import org.neo4j.gds.core.loading.construction.GraphFactory;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;
import org.neo4j.gds.steiner.ShortestPathsSteinerAlgorithm;
import org.neo4j.gds.steiner.SteinerTreeAlgorithmFactory;
import org.neo4j.gds.steiner.SteinerTreeMutateConfig;
import org.neo4j.gds.steiner.SteinerTreeResult;

import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.STREAM;

@GdsCallable(name = "gds.beta.SteinerTree.mutate", description = SteinerTreeStatsProc.DESCRIPTION, executionMode = STREAM)
public class SteinerTreeMutateSpec implements AlgorithmSpec<ShortestPathsSteinerAlgorithm, SteinerTreeResult, SteinerTreeMutateConfig, Stream<MutateResult>, SteinerTreeAlgorithmFactory<SteinerTreeMutateConfig>> {

    @Override
    public String name() {
        return "SteinerTreeMutate";
    }

    @Override
    public SteinerTreeAlgorithmFactory<SteinerTreeMutateConfig> algorithmFactory() {
        return new SteinerTreeAlgorithmFactory<>();
    }

    @Override
    public NewConfigFunction<SteinerTreeMutateConfig> newConfigFunction() {
        return (__, config) -> SteinerTreeMutateConfig.of(config);

    }

    public ComputationResultConsumer<ShortestPathsSteinerAlgorithm, SteinerTreeResult, SteinerTreeMutateConfig, Stream<MutateResult>> computationResultConsumer() {
        return (computationResult, executionContext) -> {
            Graph graph = computationResult.graph();
            var steinerTreeResult = computationResult.result();
            var config = computationResult.config();

            MutateResult.Builder builder = new MutateResult.Builder();

            if (graph.isEmpty()) {
                graph.release();
                return Stream.of(builder.build());
            }

            var mutateRelationshipType = RelationshipType.of(config.mutateRelationshipType());
            var relationshipsBuilder = GraphFactory
                .initRelationshipsBuilder()
                .nodes(computationResult.graph())
                .relationshipType(mutateRelationshipType)
                .addPropertyConfig(GraphFactory.PropertyConfig.of(config.mutateProperty()))
                .orientation(Orientation.NATURAL)
                .build();

            builder
                .withTotalWeight(steinerTreeResult.totalCost())
                .withEffectiveNodeCount(steinerTreeResult.effectiveNodeCount());


            SingleTypeRelationships relationships;

            var sourceNode = config.sourceNode();


            try (ProgressTimer ignored = ProgressTimer.start(builder::withMutateMillis)) {

                var parentArray=steinerTreeResult.parentArray();
                var costArray=steinerTreeResult.relationshipToParentCost();
                LongStream.range(0, graph.nodeCount())
                    .filter(nodeId -> parentArray.get(nodeId) != ShortestPathsSteinerAlgorithm.PRUNED)
                    .forEach(nodeId -> {
                        var sourceNodeId = (sourceNode == graph.toOriginalNodeId(nodeId)) ?
                            nodeId :
                            parentArray.get(nodeId);

                        if (nodeId != sourceNodeId) {
                            relationshipsBuilder.addFromInternal(sourceNodeId, nodeId, costArray.get(nodeId));
                        }
                    });

            }
            relationships = relationshipsBuilder.build();

            computationResult
                .graphStore()
                .addRelationshipType(mutateRelationshipType, relationships);
            builder
                .withEffectiveTargetNodeCount(steinerTreeResult.effectiveTargetNodesCount())
                .withComputeMillis(computationResult.computeMillis())
                .withPreProcessingMillis(computationResult.preProcessingMillis())
                .withRelationshipsWritten(steinerTreeResult.effectiveNodeCount() - 1)
                .withConfig(config);

            return Stream.of(builder.build());
        };

//        return (computationResult, executionContext) -> {
//            var sourceNode = computationResult.config().sourceNode();
//            Graph graph = computationResult.graph();
//            var steinerTreeResult = computationResult.result();
//            var parentArray=steinerTreeResult.parentArray();
//            var costArray=steinerTreeResult.relationshipToParentCost();
//            return Stream.empty();
//        };
    }
}
