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
package org.neo4j.gds.similarity.nodesim;

import org.HdrHistogram.DoubleHistogram;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.api.ProcedureReturnColumns;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.api.schema.RelationshipPropertySchema;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.huge.HugeGraph;
import org.neo4j.gds.core.loading.SingleTypeRelationships;
import org.neo4j.gds.core.loading.construction.GraphFactory;
import org.neo4j.gds.core.loading.construction.RelationshipsBuilder;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;
import org.neo4j.gds.similarity.SimilarityGraphResult;
import org.neo4j.gds.similarity.SimilarityMutateResult;
import org.neo4j.gds.similarity.SimilarityProc;
import org.neo4j.gds.similarity.SimilarityResultBuilder;

import java.util.Collections;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.gds.LoggingUtil.runWithExceptionLogging;
import static org.neo4j.gds.core.ProcedureConstants.HISTOGRAM_PRECISION_DEFAULT;
import static org.neo4j.gds.executor.ExecutionMode.MUTATE_RELATIONSHIP;
import static org.neo4j.gds.similarity.SimilarityProc.computeHistogram;
import static org.neo4j.gds.similarity.SimilarityProc.shouldComputeHistogram;
import static org.neo4j.gds.similarity.nodesim.NodeSimilarityProc.NODE_SIMILARITY_DESCRIPTION;

@GdsCallable(name = "gds.nodeSimilarity.mutate", description = NODE_SIMILARITY_DESCRIPTION, executionMode = MUTATE_RELATIONSHIP)
public class NodeSimilarityMutateSpecification implements AlgorithmSpec<NodeSimilarity, NodeSimilarityResult, NodeSimilarityMutateConfig, Stream<SimilarityMutateResult>, NodeSimilarityFactory<NodeSimilarityMutateConfig>> {
    @Override
    public String name() {
        return "NodeSimilarityMutate";
    }

    @Override
    public NodeSimilarityFactory<NodeSimilarityMutateConfig> algorithmFactory() {
        return new NodeSimilarityFactory<>();
    }

    @Override
    public NewConfigFunction<NodeSimilarityMutateConfig> newConfigFunction() {
        return (__, userInput) -> NodeSimilarityMutateConfig.of(userInput);
    }

    @Override
    public ComputationResultConsumer<NodeSimilarity, NodeSimilarityResult, NodeSimilarityMutateConfig, Stream<SimilarityMutateResult>> computationResultConsumer() {
        return (computationResult, executionContext) -> runWithExceptionLogging("Graph mutation failed", executionContext.log(), ()  -> {
            NodeSimilarityMutateConfig config = computationResult.config();

            if (computationResult.isGraphEmpty()) {
                return Stream.of(
                    new SimilarityMutateResult(
                        computationResult.preProcessingMillis(),
                        0,
                        0,
                        0,
                        0,
                        0,
                        Collections.emptyMap(),
                        config.toMap()
                    )
                );
            }

            SimilarityResultBuilder<SimilarityMutateResult> resultBuilder =
                SimilarityProc.withGraphsizeAndTimings(new SimilarityMutateResult.Builder(), computationResult, NodeSimilarityResult::graphResult);

            try (ProgressTimer ignored = ProgressTimer.start(resultBuilder::withMutateMillis)) {
                RelationshipType relationshipType = RelationshipType.of(config.mutateRelationshipType());

                var resultRelationships = getRelationships(
                    relationshipType,
                    computationResult,
                    computationResult.result()
                        .map(NodeSimilarityResult::graphResult)
                        .orElseGet(() -> new SimilarityGraphResult(computationResult.graph(), 0, false)),
                    config.mutateProperty(),
                    resultBuilder,
                    executionContext.returnColumns()
                );

                computationResult
                    .graphStore()
                    .addRelationshipType(resultRelationships);
            }
            return Stream.of(resultBuilder.build());
        });
    }

    private SingleTypeRelationships getRelationships(
        RelationshipType relationshipType, ComputationResult<NodeSimilarity, NodeSimilarityResult, NodeSimilarityMutateConfig> computationResult,
        SimilarityGraphResult similarityGraphResult,
        String relationshipPropertyKey,
        SimilarityResultBuilder<SimilarityMutateResult> resultBuilder,
        ProcedureReturnColumns returnColumns
    ) {
        SingleTypeRelationships relationships;

        if (similarityGraphResult.isTopKGraph()) {
            TopKGraph topKGraph = (TopKGraph) similarityGraphResult.similarityGraph();

            RelationshipsBuilder relationshipsBuilder = GraphFactory.initRelationshipsBuilder()
                .nodes(topKGraph)
                .relationshipType(relationshipType)
                .orientation(Orientation.NATURAL)
                .addPropertyConfig(GraphFactory.PropertyConfig.of(relationshipPropertyKey))
                .concurrency(1)
                .executorService(Pools.DEFAULT)
                .build();

            IdMap idMap = computationResult.graph();

            if (shouldComputeHistogram(returnColumns)) {
                DoubleHistogram histogram = new DoubleHistogram(HISTOGRAM_PRECISION_DEFAULT);
                topKGraph.forEachNode(nodeId -> {
                    topKGraph.forEachRelationship(nodeId, Double.NaN, (sourceNodeId, targetNodeId, property) -> {
                        relationshipsBuilder.addFromInternal(idMap.toRootNodeId(sourceNodeId), idMap.toRootNodeId(targetNodeId), property);
                        histogram.recordValue(property);
                        return true;
                    });
                    return true;
                });
                resultBuilder.withHistogram(histogram);
            } else {
                topKGraph.forEachNode(nodeId -> {
                    topKGraph.forEachRelationship(nodeId, Double.NaN, (sourceNodeId, targetNodeId, property) -> {
                        relationshipsBuilder.addFromInternal(idMap.toRootNodeId(sourceNodeId), idMap.toRootNodeId(targetNodeId), property);
                        return true;
                    });
                    return true;
                });
            }
            relationships = relationshipsBuilder.build();
        } else {
            HugeGraph similarityGraph = (HugeGraph) similarityGraphResult.similarityGraph();

            relationships = SingleTypeRelationships.of(
                relationshipType,
                similarityGraph.relationshipTopology(),
                similarityGraph.schema().direction(),
                similarityGraph.relationshipProperties(),
                Optional.of(RelationshipPropertySchema.of(relationshipPropertyKey, ValueType.DOUBLE))
            );

            if (shouldComputeHistogram(returnColumns)) {
                resultBuilder.withHistogram(computeHistogram(similarityGraph));
            }
        }
        return relationships;
    }

}
