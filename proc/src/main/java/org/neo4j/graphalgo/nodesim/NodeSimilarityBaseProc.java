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
import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.AlgorithmFactory;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.write.RelationshipExporter;
import org.neo4j.graphalgo.newapi.GraphCreateConfig;

import java.util.Collections;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.graphdb.Direction.OUTGOING;

public abstract class NodeSimilarityBaseProc<CONFIG extends NodeSimilarityBaseConfig> extends AlgoBaseProc<NodeSimilarity, NodeSimilarityResult, CONFIG> {

    static final String NODE_SIMILARITY_DESCRIPTION =
        "The Node Similarity algorithm compares a set of nodes based on the nodes they are connected to. " +
        "Two nodes are considered similar if they share many of the same neighbors. " +
        "Node Similarity computes pair-wise similarities based on the Jaccard metric.";

    protected abstract CONFIG newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    );

    @Override
    protected AlgorithmFactory<NodeSimilarity, CONFIG> algorithmFactory(CONFIG config) {
        return new NodeSimilarityFactory<>();
    }

    public Stream<NodeSimilarityWriteProc.NodeSimilarityWriteResult> write(
        ComputationResult<NodeSimilarity, NodeSimilarityResult, CONFIG> computationResult
    ) {
        CONFIG config = computationResult.config();
        boolean write = config instanceof NodeSimilarityWriteConfig;
        NodeSimilarityWriteConfig writeConfig = ImmutableNodeSimilarityWriteConfig.builder()
            .writeProperty("stats does not support a write property")
            .from(config)
            .build();
        if (computationResult.isGraphEmpty()) {
            return Stream.of(
                new NodeSimilarityWriteProc.NodeSimilarityWriteResult(
                    writeConfig,
                    computationResult.createMillis(),
                    0,
                    0,
                    0,
                    0,
                    0,
                    Collections.emptyMap()
                )
            );
        }

        String writeRelationshipType = writeConfig.writeRelationshipType();
        String writeProperty = writeConfig.writeProperty();
        NodeSimilarityResult result = computationResult.result();
        NodeSimilarity algorithm = computationResult.algorithm();
        SimilarityGraphResult similarityGraphResult = result.maybeGraphResult().get();
        Graph similarityGraph = similarityGraphResult.similarityGraph();

        NodeSimilarityWriteProc.WriteResultBuilder resultBuilder = new NodeSimilarityWriteProc.WriteResultBuilder(
            writeConfig);
        resultBuilder
            .withNodesCompared(similarityGraphResult.comparedNodes())
            .withRelationshipsWritten(similarityGraphResult.similarityGraph().relationshipCount());
        resultBuilder.withCreateMillis(computationResult.createMillis());
        resultBuilder.withComputeMillis(computationResult.computeMillis());

        boolean shouldComputeHistogram = callContext
            .outputFields()
            .anyMatch(s -> s.equalsIgnoreCase("similarityDistribution"));
        if (write && similarityGraph.relationshipCount() > 0) {
            runWithExceptionLogging(
                "NodeSimilarity write-back failed",
                () -> {
                    try (ProgressTimer ignored = ProgressTimer.start(resultBuilder::withWriteMillis)) {
                        RelationshipExporter exporter = RelationshipExporter
                            .of(
                                api,
                                similarityGraph,
                                similarityGraph.getLoadDirection(),
                                algorithm.getTerminationFlag()
                            )
                            .withLog(log)
                            .build();
                        if (shouldComputeHistogram) {
                            DoubleHistogram histogram = new DoubleHistogram(5);
                            exporter.write(
                                writeRelationshipType,
                                writeProperty,
                                (node1, node2, similarity) -> {
                                    histogram.recordValue(similarity);
                                    return true;
                                }
                            );
                            resultBuilder.withHistogram(histogram);
                        } else {
                            exporter.write(writeRelationshipType, writeProperty);
                        }
                    }
                }
            );
        } else if (shouldComputeHistogram) {
            try (ProgressTimer ignored = resultBuilder.timePostProcessing()) {
                resultBuilder.withHistogram(computeHistogram(similarityGraph));
            }
        }
        return Stream.of(resultBuilder.build());
    }

    private DoubleHistogram computeHistogram(Graph similarityGraph) {
        DoubleHistogram histogram = new DoubleHistogram(5);
        similarityGraph.forEachNode(nodeId -> {
            similarityGraph.forEachRelationship(nodeId, OUTGOING, Double.NaN, (node1, node2, property) -> {
                histogram.recordValue(property);
                return true;
            });
            return true;
        });
        return histogram;
    }
}
