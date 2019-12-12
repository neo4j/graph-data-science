/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.compat.MapUtil;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.write.RelationshipExporter;
import org.neo4j.graphalgo.impl.nodesim.NodeSimilarity;
import org.neo4j.graphalgo.impl.nodesim.NodeSimilarityResult;
import org.neo4j.graphalgo.impl.nodesim.NodeSimilarityWriteConfig;
import org.neo4j.graphalgo.impl.nodesim.SimilarityGraphResult;
import org.neo4j.graphalgo.impl.results.AbstractResultBuilder;
import org.neo4j.graphalgo.impl.results.MemoryEstimateResult;
import org.neo4j.graphalgo.newapi.GraphCreateConfig;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.graphdb.Direction.OUTGOING;
import static org.neo4j.procedure.Mode.READ;

public class NodeSimilarityWriteProc extends NodeSimilarityProcBase<NodeSimilarityWriteConfig> {

    @Procedure(name = "gds.algo.nodeSimilarity.write", mode = Mode.WRITE)
    @Description("CALL gds.algo.nodeSimilarity.write(graphName: STRING, configuration: MAP {" +
                 "    similarityCutoff: 0.0," +
                 "    degreeCutoff: 0," +
                 "    topK: 10," +
                 "    bottomK: 10," +
                 "    topN: 0," +
                 "    bottomN: 0," +
                 "    concurrency: 4," +
                 "    readConcurrency: 4," +
                 "    writeRelationshipType: ," +
                 "    writeProperty: ," +
                 "    writeConcurrency: 4" +
                 "  }" +
                 ") YIELD" +
                 "  nodesCompared," +
                 "  relationships," +
                 "  writeRelationshipType," +
                 "  writeProperty," +
                 "  similarityDistribution")
    public Stream<NodeSimilarityWriteResult> write(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ComputationResult<NodeSimilarity, NodeSimilarityResult, NodeSimilarityWriteConfig> result = compute(
            graphNameOrConfig,
            configuration
        );
        return write(result, true);
    }

    @Procedure(value = "gds.algo.nodeSimilarity.write.estimate", mode = READ)
    @Description("CALL gds.algo.nodeSimilarity.write.estimate(graphName: STRING, configuration: MAP {" +
                 "    similarityCutoff: 0.0," +
                 "    degreeCutoff: 0," +
                 "    topK: 10," +
                 "    bottomK: 10," +
                 "    topN: 0," +
                 "    bottomN: 0," +
                 "    concurrency: 4," +
                 "    readConcurrency: 4," +
                 "    writeRelationshipType: ," +
                 "    writeProperty: ," +
                 "    writeConcurrency: 4" +
                 "  }" +
                 ") YIELD" +
                 "  nodes: INTEGER, "+
                 "  relationships: INTEGER," +
                 "  bytesMin: INTEGER," +
                 "  bytesMax: INTEGER," +
                 "  requiredMemory: STRING," +
                 "  mapView: MAP," +
                 "  treeView: STRING")
    public Stream<MemoryEstimateResult> estimateWrite(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return computeMemoryEstimate(graphNameOrConfig, configuration);
    }

    @Procedure(name = "gds.algo.nodeSimilarity.stats", mode = Mode.WRITE)
    @Description("CALL algo.nodeSimilarity(graphName: STRING, configuration: MAP {" +
                 "    similarityCutoff: 0.0," +
                 "    degreeCutoff: 0," +
                 "    topK: 10," +
                 "    bottomK: 10," +
                 "    topN: 0," +
                 "    bottomN: 0," +
                 "    concurrency: 4," +
                 "    readConcurrency: 4," +
                 "    writeRelationshipType: ," +
                 "    writeProperty: ," +
                 "    writeConcurrency: 4" +
                 "  }" +
                 ") YIELD" +
                 "  nodesCompared," +
                 "  relationships," +
                 "  writeRelationshipType," +
                 "  writeProperty," +
                 "  similarityDistribution")
    public Stream<NodeSimilarityWriteResult> stats(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ComputationResult<NodeSimilarity, NodeSimilarityResult, NodeSimilarityWriteConfig> result = compute(
            graphNameOrConfig,
            configuration
        );
        return write(result, false);
    }

    @Procedure(value = "gds.algo.nodeSimilarity.stats.estimate", mode = READ)
    @Description("CALL gds.algo.nodeSimilarity.stats.estimate(graphName: STRING, configuration: MAP {" +
                 "    similarityCutoff: 0.0," +
                 "    degreeCutoff: 0," +
                 "    topK: 10," +
                 "    bottomK: 10," +
                 "    topN: 0," +
                 "    bottomN: 0," +
                 "    concurrency: 4," +
                 "    readConcurrency: 4," +
                 "    writeRelationshipType: ," +
                 "    writeProperty: ," +
                 "    writeConcurrency: 4" +
                 "  }" +
                 ") YIELD" +
                 "  nodes: INTEGER, "+
                 "  relationships: INTEGER," +
                 "  bytesMin: INTEGER," +
                 "  bytesMax: INTEGER," +
                 "  requiredMemory: STRING," +
                 "  mapView: MAP," +
                 "  treeView: STRING")
    public Stream<MemoryEstimateResult> estimateStats(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return computeMemoryEstimate(graphNameOrConfig, configuration);
    }

    public Stream<NodeSimilarityWriteResult> write(ComputationResult<NodeSimilarity, NodeSimilarityResult, NodeSimilarityWriteConfig> computationResult, boolean write) {
        NodeSimilarityWriteConfig config = computationResult.config();
        if (computationResult.isEmpty()) {
            return Stream.of(
                new NodeSimilarityWriteResult(
                    config,
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

        String writeRelationshipType = config.writeRelationshipType();
        String writeProperty = config.writeProperty();
        NodeSimilarityResult result = computationResult.result();
        NodeSimilarity algorithm = computationResult.algorithm();
        SimilarityGraphResult similarityGraphResult = result.maybeGraphResult().get();
        Graph similarityGraph = similarityGraphResult.similarityGraph();

        WriteResultBuilder resultBuilder = new WriteResultBuilder(config);
        resultBuilder
            .withNodesCompared(similarityGraphResult.comparedNodes())
            .withWriteProperty(config.writeProperty())
            .withRelationshipCount(similarityGraphResult.similarityGraph().relationshipCount());
        resultBuilder.setLoadMillis(computationResult.createMillis());
        resultBuilder.setComputeMillis(computationResult.computeMillis());

        boolean shouldComputeHistogram = callContext.outputFields().anyMatch(s -> s.equalsIgnoreCase("similarityDistribution"));
        if (write && similarityGraph.relationshipCount() > 0) {
            runWithExceptionLogging(
                "NodeSimilarity write-back failed",
                () -> resultBuilder.timeWrite(
                    () -> {
                        RelationshipExporter exporter = RelationshipExporter
                            .of(api, similarityGraph, similarityGraph.getLoadDirection(), algorithm.getTerminationFlag())
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
                )
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

    @Override
    protected NodeSimilarityWriteConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper userInput
    ) {
        return NodeSimilarityWriteConfig.of(username, graphName, maybeImplicitCreate, userInput);
    }

    public static class WriteResultBuilder extends AbstractResultBuilder<NodeSimilarityWriteResult> {

        private final NodeSimilarityWriteConfig config;
        private long nodesCompared = 0L;

        private long postProcessingMillis = -1L;

        private Optional<DoubleHistogram> maybeHistogram = Optional.empty();

        WriteResultBuilder(NodeSimilarityWriteConfig config) {
            this.config = config;
        }

        public WriteResultBuilder withNodesCompared(long nodesCompared) {
            this.nodesCompared = nodesCompared;
            return this;
        }

        WriteResultBuilder withHistogram(DoubleHistogram histogram) {
            this.maybeHistogram = Optional.of(histogram);
            return this;
        }

        void setPostProcessingMillis(long postProcessingMillis) {
            this.postProcessingMillis = postProcessingMillis;
        }

        ProgressTimer timePostProcessing() {
            return ProgressTimer.start(this::setPostProcessingMillis);
        }

        private Map<String, Object> distribution() {
            if (maybeHistogram.isPresent()) {
                DoubleHistogram definitelyHistogram = maybeHistogram.get();
                return MapUtil.map(
                    "min", definitelyHistogram.getMinValue(),
                    "max", definitelyHistogram.getMaxValue(),
                    "mean", definitelyHistogram.getMean(),
                    "stdDev", definitelyHistogram.getStdDeviation(),
                    "p1", definitelyHistogram.getValueAtPercentile(1),
                    "p5", definitelyHistogram.getValueAtPercentile(5),
                    "p10", definitelyHistogram.getValueAtPercentile(10),
                    "p25", definitelyHistogram.getValueAtPercentile(25),
                    "p50", definitelyHistogram.getValueAtPercentile(50),
                    "p75", definitelyHistogram.getValueAtPercentile(75),
                    "p90", definitelyHistogram.getValueAtPercentile(90),
                    "p95", definitelyHistogram.getValueAtPercentile(95),
                    "p99", definitelyHistogram.getValueAtPercentile(99),
                    "p100", definitelyHistogram.getValueAtPercentile(100)
                );
            }
            return Collections.emptyMap();
        }

        @Override
        public NodeSimilarityWriteResult build() {
            return new NodeSimilarityWriteResult(
                config,
                loadMillis,
                computeMillis,
                writeMillis,
                postProcessingMillis,
                nodesCompared,
                relationshipCount,
                distribution()
            );
        }
    }

    public static class NodeSimilarityWriteResult {
        public final long loadMillis;
        public final long computeMillis;
        public final long writeMillis;
        public final long postProcessingMillis;

        public final long nodesCompared;
        public final long relationships;
        public final String writeRelationshipType;
        public final String writeProperty;

        public final Map<String, Object> similarityDistribution;

        NodeSimilarityWriteResult(
            NodeSimilarityWriteConfig config,
            long loadMillis,
            long computeMillis,
            long writeMillis,
            long postProcessingMillis,
            long nodesCompared,
            long relationships,
            Map<String, Object> similarityDistribution
        ) {
            this.loadMillis = loadMillis;
            this.computeMillis = computeMillis;
            this.writeMillis = writeMillis;
            this.postProcessingMillis = postProcessingMillis;
            this.nodesCompared = nodesCompared;
            this.relationships = relationships;
            this.writeRelationshipType = config.writeRelationshipType();
            this.writeProperty = config.writeProperty();
            this.similarityDistribution = similarityDistribution;
        }
    }
}
