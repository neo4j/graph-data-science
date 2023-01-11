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

import org.HdrHistogram.DoubleHistogram;
import org.neo4j.gds.MutateComputationResultConsumer;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.api.schema.RelationshipPropertySchema;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.huge.HugeGraph;
import org.neo4j.gds.core.loading.SingleTypeRelationshipImportResult;
import org.neo4j.gds.core.loading.construction.GraphFactory;
import org.neo4j.gds.core.loading.construction.RelationshipsBuilder;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.ExecutionMode;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;
import org.neo4j.gds.result.AbstractResultBuilder;
import org.neo4j.gds.similarity.SimilarityGraphResult;
import org.neo4j.gds.similarity.SimilarityMutateResult;
import org.neo4j.gds.similarity.SimilarityProc;
import org.neo4j.gds.similarity.nodesim.NodeSimilarity;
import org.neo4j.gds.similarity.nodesim.NodeSimilarityResult;
import org.neo4j.gds.similarity.nodesim.TopKGraph;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;

import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.gds.core.ProcedureConstants.HISTOGRAM_PRECISION_DEFAULT;
import static org.neo4j.gds.similarity.SimilarityProc.computeHistogram;
import static org.neo4j.gds.similarity.SimilarityProc.shouldComputeHistogram;
import static org.neo4j.gds.similarity.filterednodesim.FilteredNodeSimilarityStreamProc.DESCRIPTION;

@GdsCallable(name = "gds.alpha.nodeSimilarity.filtered.mutate", description = DESCRIPTION, executionMode = ExecutionMode.MUTATE_RELATIONSHIP)
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
    public FilteredNodeSimilarityFactory<FilteredNodeSimilarityMutateConfig> algorithmFactory() {
        return new FilteredNodeSimilarityFactory<>();
    }

    @Override
    public NewConfigFunction<FilteredNodeSimilarityMutateConfig> newConfigFunction() {
        return (__, config) -> FilteredNodeSimilarityMutateConfig.of(config);
    }

    protected AbstractResultBuilder<SimilarityMutateResult> resultBuilder(
        ComputationResult<NodeSimilarity, NodeSimilarityResult, FilteredNodeSimilarityMutateConfig> computationResult,
        ExecutionContext executionContext
    ) {

        SimilarityProc.SimilarityResultBuilder<SimilarityMutateResult> resultBuilder =
            SimilarityProc.withGraphsizeAndTimings(new SimilarityMutateResult.Builder(), computationResult, NodeSimilarityResult::graphResult);
        return resultBuilder;
    }

    @Override
    public ComputationResultConsumer<NodeSimilarity, NodeSimilarityResult, FilteredNodeSimilarityMutateConfig, Stream<SimilarityMutateResult>> computationResultConsumer() {
        return new MutateComputationResultConsumer<>(this::resultBuilder) {
            @Override
            protected void updateGraphStore(
                AbstractResultBuilder<?> resultBuilder,
                ComputationResult<NodeSimilarity, NodeSimilarityResult, FilteredNodeSimilarityMutateConfig> computationResult,
                ExecutionContext executionContext
            ) {
                var config = computationResult.config();

                var relationships = getRelationships(
                    computationResult,
                    computationResult.result().graphResult(),
                    config.mutateProperty(),
                    (SimilarityProc.SimilarityResultBuilder<SimilarityMutateResult>) resultBuilder,
                    executionContext.callContext()
                );

                var relationshipType = RelationshipType.of(config.mutateRelationshipType());

                computationResult
                    .graphStore()
                    .addRelationshipType(relationshipType, relationships);

                resultBuilder.withRelationshipsWritten(relationships.topology().elementCount());
            }
        };
    }

    private SingleTypeRelationshipImportResult getRelationships(
        ComputationResult<NodeSimilarity, NodeSimilarityResult, FilteredNodeSimilarityMutateConfig> computationResult,
        SimilarityGraphResult similarityGraphResult,
        String relationshipPropertyKey,
        SimilarityProc.SimilarityResultBuilder<SimilarityMutateResult> resultBuilder,
        ProcedureCallContext callContext
    ) {
        SingleTypeRelationshipImportResult resultRelationships;

        if (similarityGraphResult.isTopKGraph()) {
            TopKGraph topKGraph = (TopKGraph) similarityGraphResult.similarityGraph();

            RelationshipsBuilder relationshipsBuilder = GraphFactory.initRelationshipsBuilder()
                .nodes(topKGraph)
                .orientation(Orientation.NATURAL)
                .addPropertyConfig(GraphFactory.PropertyConfig.of(relationshipPropertyKey))
                .concurrency(1)
                .executorService(Pools.DEFAULT)
                .build();

            IdMap idMap = computationResult.graph();

            if (shouldComputeHistogram(callContext)) {
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
            resultRelationships = relationshipsBuilder.build();
        } else {
            HugeGraph similarityGraph = (HugeGraph) similarityGraphResult.similarityGraph();

            resultRelationships = SingleTypeRelationshipImportResult.of(
                similarityGraph.relationships(),
                similarityGraph.schema().direction(),
                Optional.of(RelationshipPropertySchema.of(relationshipPropertyKey, ValueType.DOUBLE))
            );
            if (shouldComputeHistogram(callContext)) {
                resultBuilder.withHistogram(computeHistogram(similarityGraph));
            }
        }
        return resultRelationships;
    }
}
