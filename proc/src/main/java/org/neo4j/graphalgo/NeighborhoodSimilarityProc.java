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

package org.neo4j.graphalgo;

import org.HdrHistogram.DoubleHistogram;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.mem.MemoryTreeWithDimensions;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.write.RelationshipExporter;
import org.neo4j.graphalgo.impl.jaccard.NeighborhoodSimilarity;
import org.neo4j.graphalgo.impl.jaccard.NeighborhoodSimilarityFactory;
import org.neo4j.graphalgo.impl.jaccard.SimilarityGraphResult;
import org.neo4j.graphalgo.impl.jaccard.SimilarityResult;
import org.neo4j.graphalgo.impl.results.AbstractResultBuilder;
import org.neo4j.graphalgo.impl.results.MemRecResult;
import org.neo4j.graphdb.Direction;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.graphdb.Direction.OUTGOING;

public class NeighborhoodSimilarityProc extends BaseAlgoProc<NeighborhoodSimilarity> {

    private static final String SIMILARITY_CUTOFF_KEY = "similarityCutoff";
    private static final double SIMILARITY_CUTOFF_DEFAULT = 1E-42;

    private static final String DEGREE_CUTOFF_KEY = "degreeCutoff";
    private static final int DEGREE_CUTOFF_DEFAULT = 1;

    private static final String TOP_N_KEY = "topN";
    private static final int TOP_N_DEFAULT = 0;

    private static final String TOP_K_KEY = "topK";
    private static final int TOP_K_DEFAULT = 10;

    private static final String BOTTOM_N_KEY = "bottomN";
    private static final int BOTTOM_N_DEFAULT = TOP_N_DEFAULT;

    private static final String BOTTOM_K_KEY = "bottomK";
    private static final int BOTTOM_K_DEFAULT = TOP_K_DEFAULT;

    private static final String WRITE_RELATIONSHIP_TYPE_KEY = "writeRelationshipType";
    private static final String WRITE_RELATIONSHIP_TYPE_DEFAULT = "SIMILAR";

    private static final String WRITE_PROPERTY_KEY = "writeProperty";
    private static final String WRITE_PROPERTY_DEFAULT = "score";
    private static final double WRITE_PROPERTY_VALUE_DEFAULT = 0.0;

    private static final Direction COMPUTE_DIRECTION_DEFAULT = OUTGOING;

    @Procedure(name = "algo.beta.jaccard.stream", mode = Mode.READ)
    @Description("CALL algo.beta.jaccard.stream(" +
                 "nodeFilter, relationshipFilter, {" +
                 "  similarityCutoff: 0.0, degreeCutoff: 0," +
                 "  topK: 10, bottomK: 10, topN: 0, bottomN: 0," +
                 "  graph: 'graph', direction: 'OUTGOING', concurrency: 4, readConcurrency: 4" +
                 "}) " +
                 "YIELD node1, node2, similarity - computes neighborhood similarities based on the Jaccard index")
    public Stream<SimilarityResult> stream(
        @Name(value = "nodeFilter", defaultValue = "") String nodeFilter,
        @Name(value = "relationshipFilter", defaultValue = "") String relationshipFilter,
        @Name(value = "config", defaultValue = "{}") Map<String, Object> config
    ) {
        AllocationTracker tracker = AllocationTracker.create();
        ProcedureConfiguration configuration = newConfig(nodeFilter, relationshipFilter, config);
        Graph graph = loadGraph(configuration, tracker, new WriteResultBuilder());

        if (graph.isEmpty()) {
            graph.release();
            return Stream.empty();
        }

        NeighborhoodSimilarity neighborhoodSimilarity = newAlgorithm(graph, configuration, tracker);

        Direction direction = configuration.getDirection(COMPUTE_DIRECTION_DEFAULT);
        return runWithExceptionLogging(
            "NeighborhoodSimilarity compute failed",
            () -> neighborhoodSimilarity.computeToStream(direction)
        );
    }

    @Procedure(name = "algo.beta.jaccard", mode = Mode.WRITE)
    @Description("CALL algo.beta.jaccard(" +
                 "nodeFilter, relationshipFilter, {" +
                 "  similarityCutoff: 0.0, degreeCutoff: 0," +
                 "  topK: 10, bottomK: 10, topN: 0, bottomN: 0," +
                 "  graph: 'graph', direction: 'OUTGOING', concurrency: 4, readConcurrency: 4," +
                 "  write: 'true', writeRelationshipType: 'SIMILAR_TO', writeProperty: 'similarity', writeConcurrency: 4" +
                 "}) " +
                 "YIELD nodesCompared, relationships, write, writeRelationshipType, writeProperty - computes neighborhood similarities based on the Jaccard index")
    public Stream<NeighborhoodSimilarityResult> write(
        @Name(value = "nodeFilter", defaultValue = "") String nodeFilter,
        @Name(value = "relationshipFilter", defaultValue = "") String relationshipFilter,
        @Name(value = "config", defaultValue = "{}") Map<String, Object> config
    ) {
        WriteResultBuilder resultBuilder = new WriteResultBuilder();
        AllocationTracker tracker = AllocationTracker.create();
        ProcedureConfiguration configuration = newConfig(nodeFilter, relationshipFilter, config);
        Graph graph = loadGraph(configuration, tracker, resultBuilder);

        String writeRelationshipType = configuration.get(WRITE_RELATIONSHIP_TYPE_KEY, WRITE_RELATIONSHIP_TYPE_DEFAULT);
        String writeProperty = configuration.get(WRITE_PROPERTY_KEY, WRITE_PROPERTY_DEFAULT);

        resultBuilder
            .withWriteRelationshipType(writeRelationshipType)
            .withWrite(configuration.isWriteFlag())
            .withWriteProperty(writeProperty);

        if (graph.isEmpty()) {
            graph.release();
            return Stream.of(resultBuilder.build());
        }

        NeighborhoodSimilarity neighborhoodSimilarity = newAlgorithm(graph, configuration, tracker);

        Direction direction = configuration.getDirection(COMPUTE_DIRECTION_DEFAULT);
        SimilarityGraphResult similarityGraphResult = runWithExceptionLogging(
            "NeighborhoodSimilarity compute failed",
            () -> resultBuilder.timeEval(() -> neighborhoodSimilarity.computeToGraph(direction))
        );
        graph.releaseTopology();

        Graph similarityGraph = similarityGraphResult.similarityGraph();
        resultBuilder
            .withNodesCompared(similarityGraphResult.comparedNodes())
            .withRelationshipCount(similarityGraph.relationshipCount());

        if (configuration.isWriteFlag() && similarityGraph.relationshipCount() > 0) {
            runWithExceptionLogging(
                "NeighborhoodSimilarity write-back failed",
                () -> resultBuilder.timeWrite(
                    () -> {
                        RelationshipExporter exporter = RelationshipExporter
                            .of(api, similarityGraph, similarityGraph.getLoadDirection(), neighborhoodSimilarity.terminationFlag)
                            .withLog(log)
                            .build();
                        if (configuration.computeHistogram()) {
                            DoubleHistogram histogram = new DoubleHistogram(5);
                            exporter.write(
                                writeRelationshipType,
                                writeProperty,
                                WRITE_PROPERTY_VALUE_DEFAULT,
                                (node1, node2, similarity) -> {
                                    histogram.recordValue(similarity);
                                    return true;
                                }
                            );
                            resultBuilder.withHistogram(histogram);
                        } else {
                            exporter.write(writeRelationshipType, writeProperty, WRITE_PROPERTY_VALUE_DEFAULT);
                        }
                    }
                )
            );
        } else if (configuration.computeHistogram()) {
            try (ProgressTimer ignored = resultBuilder.timePostProcessing()) {
                resultBuilder.withHistogram(computeHistogram(similarityGraph));
            }
        }
        return Stream.of(resultBuilder.build());
    }

    private DoubleHistogram computeHistogram(Graph similarityGraph) {
        DoubleHistogram histogram = new DoubleHistogram(5);
        similarityGraph.forEachNode(nodeId -> {
                similarityGraph.forEachRelationship(nodeId, OUTGOING, 0.0, (node1, node2, property) -> {
                    histogram.recordValue(property);
                    return true;
                });
                return true;
            }
        );
        return histogram;
    }

    @Procedure(value = "algo.beta.jaccard.memrec")
    public Stream<MemRecResult> memrec(
        @Name(value = "nodeFilter", defaultValue = "") String nodeFilter,
        @Name(value = "relationshipFilter", defaultValue = "") String relationshipFilter,
        @Name(value = "config", defaultValue = "{}") Map<String, Object> config
    ) {
        ProcedureConfiguration configuration = newConfig(nodeFilter, relationshipFilter, config);
        MemoryTreeWithDimensions memoryEstimation = this.memoryEstimation(configuration);
        return Stream.of(new MemRecResult(memoryEstimation));
    }

    @Override
    protected GraphLoader configureAlgoLoader(GraphLoader loader, ProcedureConfiguration config) {
        return loader;
    }

    @Override
    protected AlgorithmFactory<NeighborhoodSimilarity> algorithmFactory(ProcedureConfiguration config) {
        // TODO: Should check if we are writing or streaming, but how to do that in memrec?
        boolean computesSimilarityGraph = true;
        return new NeighborhoodSimilarityFactory(
            config(config),
            computesSimilarityGraph
        );
    }

    NeighborhoodSimilarity.Config config(ProcedureConfiguration procedureConfiguration) {
        validTopBottom(procedureConfiguration);
        int topK = validK(procedureConfiguration);
        int topN = validN(procedureConfiguration);
        int degreeCutoff = validDegreeCutoff(procedureConfiguration);
        double similarityCutoff = procedureConfiguration
            .getNumber(SIMILARITY_CUTOFF_KEY, SIMILARITY_CUTOFF_DEFAULT)
            .doubleValue();
        int concurrency = procedureConfiguration.getConcurrency();
        int batchSize = procedureConfiguration.getBatchSize();
        return new NeighborhoodSimilarity.Config(similarityCutoff, degreeCutoff, topN, topK, concurrency, batchSize);
    }

    private void validTopBottom(ProcedureConfiguration config) {
        if (config.containsKey(TOP_K_KEY) && config.containsKey(BOTTOM_K_KEY)) {
            throw new IllegalArgumentException(String.format("Invalid parameter combination: %s combined with %s", TOP_K_KEY, BOTTOM_K_KEY));
        }
        if (config.containsKey(TOP_N_KEY) && config.containsKey(BOTTOM_N_KEY)) {
            throw new IllegalArgumentException(String.format("Invalid parameter combination: %s combined with %s", TOP_N_KEY, BOTTOM_N_KEY));
        }
    }

    private int validK(ProcedureConfiguration config) {
        boolean isBottomK = config.containsKey(BOTTOM_K_KEY);
        String message = "Invalid value for %s: must be a positive integer";
        if (isBottomK) {
            int bottomK = config.getInt(BOTTOM_K_KEY, BOTTOM_K_DEFAULT);
            if (bottomK < 1) {
                throw new IllegalArgumentException(String.format(message, BOTTOM_K_KEY));
            }
            // The algorithm only knows 'topK', if it is negative it will sort ascending
            return -bottomK;
        } else {
            int topK = config.getInt(TOP_K_KEY, TOP_K_DEFAULT);
            if (topK < 1) {
                throw new IllegalArgumentException(String.format(message, TOP_K_KEY));
            }
            return topK;
        }
    }

    private int validN(ProcedureConfiguration config) {
        boolean isTopN = config.containsKey(TOP_N_KEY);
        String message = "Invalid value for %s: must be a positive integer or zero";
        if (isTopN) {
            int topN = config.getInt(TOP_N_KEY, TOP_N_DEFAULT);
            if (topN < 0) {
                throw new IllegalArgumentException(String.format(message, TOP_N_KEY));
            }
            return topN;
        } else {
            int bottomN = config.getInt(BOTTOM_N_KEY, BOTTOM_N_DEFAULT);
            if (bottomN < 0) {
                throw new IllegalArgumentException(String.format(message, BOTTOM_N_KEY));
            }
            // The algorithm only knows 'topN', if it is negative it will sort ascending
            return -bottomN;
        }
    }

    private int validDegreeCutoff(ProcedureConfiguration config) {
        int degreeCutoff = config.getInt(DEGREE_CUTOFF_KEY, DEGREE_CUTOFF_DEFAULT);
        if (degreeCutoff < 1) {
            throw new IllegalArgumentException("Must set degree cutoff to 1 or greater");
        }
        return degreeCutoff;
    }

    public static class NeighborhoodSimilarityResult {
        public final long loadMillis;
        public final long computeMillis;
        public final long writeMillis;
        public final long postProcessingMillis;

        public final long nodesCompared;
        public final long relationships;
        public final boolean write;
        public final String writeRelationshipType;
        public final String writeProperty;

        public final double min;
        public final double max;
        public final double mean;
        public final double stdDev;
        public final double p1;
        public final double p5;
        public final double p10;
        public final double p25;
        public final double p50;
        public final double p75;
        public final double p90;
        public final double p95;
        public final double p99;
        public final double p100;

        NeighborhoodSimilarityResult(
            long loadMillis,
            long computeMillis,
            long writeMillis,
            long postProcessingMillis,
            long nodesCompared,
            long relationships,
            boolean write,
            String writeRelationshipType,
            String writeProperty,
            double min,
            double max,
            double mean,
            double stdDev,
            double p1,
            double p5,
            double p10,
            double p25,
            double p50,
            double p75,
            double p90,
            double p95,
            double p99,
            double p100
        ) {
            this.loadMillis = loadMillis;
            this.computeMillis = computeMillis;
            this.writeMillis = writeMillis;
            this.postProcessingMillis = postProcessingMillis;
            this.nodesCompared = nodesCompared;
            this.relationships = relationships;
            this.write = write;
            this.writeRelationshipType = writeRelationshipType;
            this.writeProperty = writeProperty;
            this.min = min;
            this.max = max;
            this.mean = mean;
            this.stdDev = stdDev;
            this.p1 = p1;
            this.p5 = p5;
            this.p10 = p10;
            this.p25 = p25;
            this.p50 = p50;
            this.p75 = p75;
            this.p90 = p90;
            this.p95 = p95;
            this.p99 = p99;
            this.p100 = p100;
        }
    }

    private static class WriteResultBuilder extends AbstractResultBuilder<NeighborhoodSimilarityResult> {

        private long nodesCompared = 0L;

        private long postProcessingMillis = -1L;

        private String writeRelationshipType;
        private Optional<DoubleHistogram> maybeHistogram = Optional.empty();

        WriteResultBuilder withNodesCompared(long nodesCompared) {
            this.nodesCompared = nodesCompared;
            return this;
        }

        WriteResultBuilder withWriteRelationshipType(String writeRelationshipType) {
            this.writeRelationshipType = writeRelationshipType;
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

        @Override
        public NeighborhoodSimilarityResult build() {
            return new NeighborhoodSimilarityResult(
                loadMillis,
                computeMillis,
                writeMillis,
                postProcessingMillis,
                nodesCompared,
                relationshipCount,
                write,
                writeRelationshipType,
                writeProperty,
                maybeHistogram.map(DoubleHistogram::getMinValue).orElse(-1.0),
                maybeHistogram.map(DoubleHistogram::getMaxValue).orElse(-1.0),
                maybeHistogram.map(DoubleHistogram::getMean).orElse(-1.0),
                maybeHistogram.map(DoubleHistogram::getStdDeviation).orElse(-1.0),
                maybeHistogram.map(histogram -> histogram.getValueAtPercentile(1)).orElse(-1.0),
                maybeHistogram.map(histogram -> histogram.getValueAtPercentile(5)).orElse(-1.0),
                maybeHistogram.map(histogram -> histogram.getValueAtPercentile(10)).orElse(-1.0),
                maybeHistogram.map(histogram -> histogram.getValueAtPercentile(25)).orElse(-1.0),
                maybeHistogram.map(histogram -> histogram.getValueAtPercentile(50)).orElse(-1.0),
                maybeHistogram.map(histogram -> histogram.getValueAtPercentile(75)).orElse(-1.0),
                maybeHistogram.map(histogram -> histogram.getValueAtPercentile(90)).orElse(-1.0),
                maybeHistogram.map(histogram -> histogram.getValueAtPercentile(95)).orElse(-1.0),
                maybeHistogram.map(histogram -> histogram.getValueAtPercentile(99)).orElse(-1.0),
                maybeHistogram.map(histogram -> histogram.getValueAtPercentile(100)).orElse(-1.0)
            );
        }
    }

}
