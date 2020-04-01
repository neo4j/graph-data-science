/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.nodesim;

import org.HdrHistogram.DoubleHistogram;
import org.neo4j.graphalgo.AlgorithmFactory;
import org.neo4j.graphalgo.MutateProc;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.core.Aggregation;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.huge.HugeGraph;
import org.neo4j.graphalgo.core.loading.HugeGraphUtil;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.write.PropertyTranslator;
import org.neo4j.graphalgo.result.AbstractResultBuilder;
import org.neo4j.graphalgo.results.MemoryEstimateResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.graphalgo.nodesim.NodeSimilarityProc.NODE_SIMILARITY_DESCRIPTION;
import static org.neo4j.graphalgo.nodesim.NodeSimilarityProc.computeHistogram;
import static org.neo4j.graphalgo.nodesim.NodeSimilarityProc.shouldComputeHistogram;
import static org.neo4j.procedure.Mode.READ;

public class NodeSimilarityMutateProc extends MutateProc<NodeSimilarity, NodeSimilarityResult, NodeSimilarityMutateProc.MutateResult, NodeSimilarityMutateConfig> {

    @Procedure(name = "gds.nodeSimilarity.mutate", mode = READ)
    @Description(NODE_SIMILARITY_DESCRIPTION)
    public Stream<MutateResult> mutate(
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
    protected AlgorithmFactory<NodeSimilarity, NodeSimilarityMutateConfig> algorithmFactory(
        NodeSimilarityMutateConfig config
    ) {
        return new NodeSimilarityFactory<>();
    }

    @Override
    protected PropertyTranslator<NodeSimilarityResult> nodePropertyTranslator(
        ComputationResult<NodeSimilarity, NodeSimilarityResult, NodeSimilarityMutateConfig> computationResult
    ) {
        throw new UnsupportedOperationException("NodeSimilarity does not mutate node properties.");
    }

    @Override
    protected AbstractResultBuilder<MutateResult> resultBuilder(
        ComputationResult<NodeSimilarity, NodeSimilarityResult, NodeSimilarityMutateConfig> computeResult
    ) {
        throw new UnsupportedOperationException("NodeSimilarity handles result building individually.");
    }

    @Override
    public Stream<MutateResult> mutate(
        ComputationResult<NodeSimilarity, NodeSimilarityResult, NodeSimilarityMutateConfig> computationResult
    ) {
        NodeSimilarityMutateConfig config = computationResult.config();

        if (computationResult.isGraphEmpty()) {
            return Stream.of(
                new MutateResult(
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

        NodeSimilarityProc.NodeSimilarityResultBuilder<MutateResult> resultBuilder =
            NodeSimilarityProc.resultBuilder(new MutateResult.Builder(), computationResult);

        try (ProgressTimer ignored = ProgressTimer.start(resultBuilder::withMutateMillis)) {
            HugeGraph.Relationships resultRelationships = getRelationships(
                computationResult,
                computationResult.result().graphResult(),
                resultBuilder
            );

            computationResult
                .graphStore()
                .addRelationshipType(
                    config.mutateRelationshipType(),
                    Optional.of(config.mutateProperty()),
                    resultRelationships
                );
        }
        return Stream.of(resultBuilder.build());
    }

    private HugeGraph.Relationships getRelationships(
        ComputationResult<NodeSimilarity, NodeSimilarityResult, NodeSimilarityMutateConfig> computationResult,
        SimilarityGraphResult similarityGraphResult,
        NodeSimilarityProc.NodeSimilarityResultBuilder<MutateResult> resultBuilder
    ) {
        HugeGraph.Relationships resultRelationships;

        if (similarityGraphResult.isTopKGraph()) {
            TopKGraph topKGraph = (TopKGraph) similarityGraphResult.similarityGraph();

            HugeGraphUtil.RelationshipsBuilder relationshipsBuilder = new HugeGraphUtil.RelationshipsBuilder(
                topKGraph,
                Orientation.NATURAL,
                true,
                Aggregation.NONE,
                Pools.DEFAULT,
                computationResult.tracker()
            );

            if (shouldComputeHistogram(callContext)) {
                DoubleHistogram histogram = new DoubleHistogram(5);
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

    public static class MutateResult {
        public final long createMillis;
        public final long computeMillis;
        public final long mutateMillis;
        public final long postProcessingMillis;

        public final long nodesCompared;
        public final long relationshipsWritten;

        public final Map<String, Object> similarityDistribution;
        public final Map<String, Object> configuration;

        MutateResult(
            long createMillis,
            long computeMillis,
            long mutateMillis,
            long postProcessingMillis,
            long nodesCompared,
            long relationshipsWritten,
            Map<String, Object> similarityDistribution,
            Map<String, Object> configuration
        ) {
            this.createMillis = createMillis;
            this.computeMillis = computeMillis;
            this.mutateMillis = mutateMillis;
            this.postProcessingMillis = postProcessingMillis;
            this.nodesCompared = nodesCompared;
            this.relationshipsWritten = relationshipsWritten;
            this.similarityDistribution = similarityDistribution;
            this.configuration = configuration;
        }

        static class Builder extends NodeSimilarityProc.NodeSimilarityResultBuilder<MutateResult> {

            @Override
            public MutateResult build() {
                return new MutateResult(
                    createMillis,
                    computeMillis,
                    mutateMillis,
                    postProcessingMillis,
                    nodesCompared,
                    relationshipsWritten,
                    distribution(),
                    config.toMap()
                );
            }
        }
    }
}
