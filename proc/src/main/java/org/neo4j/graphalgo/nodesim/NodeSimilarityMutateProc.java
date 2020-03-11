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
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.core.Aggregation;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.huge.HugeGraph;
import org.neo4j.graphalgo.core.loading.HugeGraphUtil;
import org.neo4j.graphalgo.results.MemoryEstimateResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;

public class NodeSimilarityMutateProc extends NodeSimilarityWriteProc {

    @Procedure(name = "gds.beta.nodeSimilarity.mutate", mode = READ)
    @Description(NODE_SIMILARITY_DESCRIPTION)
    public Stream<WriteResult> mutate(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ComputationResult<NodeSimilarity, NodeSimilarityResult, NodeSimilarityWriteConfig> result = compute(
            graphNameOrConfig,
            configuration
        );
        return mutate(result);
    }

    @Procedure(value = "gds.beta.nodeSimilarity.mutate.estimate", mode = READ)
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

    private Stream<WriteResult> mutate(ComputationResult<NodeSimilarity, NodeSimilarityResult, NodeSimilarityWriteConfig> computationResult) {
        NodeSimilarityWriteConfig config = computationResult.config();

        if (computationResult.isGraphEmpty()) {
            return Stream.of(
                new WriteResult(
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

        NodeSimilarityResult result = computationResult.result();
        SimilarityGraphResult similarityGraphResult = result.maybeGraphResult().get();

        WriteResultBuilder resultBuilder = new WriteResultBuilder();
        resultBuilder
            .withNodesCompared(similarityGraphResult.comparedNodes())
            .withRelationshipsWritten(similarityGraphResult.similarityGraph().relationshipCount());
        resultBuilder.withCreateMillis(computationResult.createMillis());
        resultBuilder.withComputeMillis(computationResult.computeMillis());
        resultBuilder.withConfig(config);

        HugeGraph.Relationships resultRelationships = getRelationships(computationResult, similarityGraphResult, resultBuilder);

        String writeRelationshipType = config.writeRelationshipType();
        String writeProperty = config.writeProperty();

        computationResult
            .graphStore()
            .addRelationshipType(writeRelationshipType, Optional.of(writeProperty), resultRelationships);

        return Stream.of(resultBuilder.build());
    }

    private HugeGraph.Relationships getRelationships(
        ComputationResult<NodeSimilarity, NodeSimilarityResult, NodeSimilarityWriteConfig> computationResult,
        SimilarityGraphResult similarityGraphResult,
        WriteResultBuilder resultBuilder
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

            if (shouldComputeHistogram()){
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
            if (shouldComputeHistogram()) {
               resultBuilder.withHistogram(computeHistogram(similarityGraph));
            }
        }
        return resultRelationships;
    }
}
