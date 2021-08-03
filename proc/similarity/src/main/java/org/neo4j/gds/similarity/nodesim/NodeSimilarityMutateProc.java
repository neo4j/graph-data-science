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
import org.neo4j.gds.AlgorithmFactory;
import org.neo4j.gds.MutatePropertyProc;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.result.AbstractResultBuilder;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.gds.similarity.SimilarityGraphResult;
import org.neo4j.gds.similarity.SimilarityMutateResult;
import org.neo4j.gds.similarity.SimilarityProc;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.RelationshipType;
import org.neo4j.graphalgo.api.DefaultValue;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.api.Relationships;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.core.Aggregation;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.huge.HugeGraph;
import org.neo4j.graphalgo.core.loading.construction.GraphFactory;
import org.neo4j.graphalgo.core.loading.construction.RelationshipsBuilder;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.values.storable.NumberType;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.gds.similarity.SimilarityProc.computeHistogram;
import static org.neo4j.gds.similarity.SimilarityProc.shouldComputeHistogram;
import static org.neo4j.gds.similarity.nodesim.NodeSimilarityProc.NODE_SIMILARITY_DESCRIPTION;
import static org.neo4j.graphalgo.core.ProcedureConstants.HISTOGRAM_PRECISION_DEFAULT;
import static org.neo4j.procedure.Mode.READ;

public class NodeSimilarityMutateProc extends MutatePropertyProc<NodeSimilarity, NodeSimilarityResult, SimilarityMutateResult, NodeSimilarityMutateConfig> {

    @Procedure(name = "gds.nodeSimilarity.mutate", mode = READ)
    @Description(NODE_SIMILARITY_DESCRIPTION)
    public Stream<SimilarityMutateResult> mutate(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return mutate(compute(graphNameOrConfig, configuration));
    }

    @Procedure(value = "gds.nodeSimilarity.mutate.estimate", mode = READ)
    @Description(ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimateMutate(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return computeEstimate(graphNameOrConfig, configuration);
    }

    @Override
    protected NodeSimilarityMutateConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper userInput
    ) {
        return NodeSimilarityMutateConfig.of(username, graphName, maybeImplicitCreate, userInput);
    }

    @Override
    protected AlgorithmFactory<NodeSimilarity, NodeSimilarityMutateConfig> algorithmFactory() {
        return new NodeSimilarityFactory<>();
    }

    @Override
    protected NodeProperties nodeProperties(
        ComputationResult<NodeSimilarity, NodeSimilarityResult, NodeSimilarityMutateConfig> computationResult
    ) {
        throw new UnsupportedOperationException("NodeSimilarity does not mutate node properties.");
    }

    @Override
    protected AbstractResultBuilder<SimilarityMutateResult> resultBuilder(
        ComputationResult<NodeSimilarity, NodeSimilarityResult, NodeSimilarityMutateConfig> computeResult
    ) {
        throw new UnsupportedOperationException("NodeSimilarity handles result building individually.");
    }

    @Override
    public Stream<SimilarityMutateResult> mutate(
        ComputationResult<NodeSimilarity, NodeSimilarityResult, NodeSimilarityMutateConfig> computationResult
    ) {
        return runWithExceptionLogging("Graph mutation failed", () -> {
            NodeSimilarityMutateConfig config = computationResult.config();

            if (computationResult.isGraphEmpty()) {
                return Stream.of(
                    new SimilarityMutateResult(
                        computationResult.createMillis(),
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

            SimilarityProc.SimilarityResultBuilder<SimilarityMutateResult> resultBuilder =
                SimilarityProc.resultBuilder(new SimilarityMutateResult.Builder(), computationResult, NodeSimilarityResult::graphResult);

            try (ProgressTimer ignored = ProgressTimer.start(resultBuilder::withMutateMillis)) {
                Relationships resultRelationships = getRelationships(
                    computationResult,
                    computationResult.result().graphResult(),
                    resultBuilder
                );

                computationResult
                    .graphStore()
                    .addRelationshipType(
                        RelationshipType.of(config.mutateRelationshipType()),
                        Optional.of(config.mutateProperty()),
                        Optional.of(NumberType.FLOATING_POINT),
                        resultRelationships
                    );
            }
            return Stream.of(resultBuilder.build());
        });
    }

    private Relationships getRelationships(
        ComputationResult<NodeSimilarity, NodeSimilarityResult, NodeSimilarityMutateConfig> computationResult,
        SimilarityGraphResult similarityGraphResult,
        SimilarityProc.SimilarityResultBuilder<SimilarityMutateResult> resultBuilder
    ) {
        Relationships resultRelationships;

        if (similarityGraphResult.isTopKGraph()) {
            TopKGraph topKGraph = (TopKGraph) similarityGraphResult.similarityGraph();

            RelationshipsBuilder relationshipsBuilder = GraphFactory.initRelationshipsBuilder()
                .nodes(topKGraph)
                .orientation(Orientation.NATURAL)
                .addPropertyConfig(Aggregation.NONE, DefaultValue.forDouble())
                .preAggregate(false)
                .concurrency(1)
                .executorService(Pools.DEFAULT)
                .tracker(allocationTracker())
                .build();

            if (shouldComputeHistogram(callContext)) {
                DoubleHistogram histogram = new DoubleHistogram(HISTOGRAM_PRECISION_DEFAULT);
                topKGraph.forEachNode(nodeId -> {
                    topKGraph.forEachRelationship(nodeId, Double.NaN, (sourceNodeId, targetNodeId, property) -> {
                        relationshipsBuilder.addFromInternal(sourceNodeId, targetNodeId, property);
                        histogram.recordValue(property);
                        return true;
                    });
                    return true;
                });
                resultBuilder.withHistogram(histogram);
            } else {
                topKGraph.forEachNode(nodeId -> {
                    topKGraph.forEachRelationship(nodeId, Double.NaN, (sourceNodeId, targetNodeId, property) -> {
                        relationshipsBuilder.addFromInternal(sourceNodeId, targetNodeId, property);
                        return true;
                    });
                    return true;
                });
            }
            resultRelationships = relationshipsBuilder.build();
        } else {
            HugeGraph similarityGraph = (HugeGraph) similarityGraphResult.similarityGraph();
            resultRelationships = similarityGraph.relationships();
            if (shouldComputeHistogram(callContext)) {
                resultBuilder.withHistogram(computeHistogram(similarityGraph));
            }
        }
        return resultRelationships;
    }
}
