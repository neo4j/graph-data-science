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
package org.neo4j.graphalgo.knn;

import org.HdrHistogram.DoubleHistogram;
import org.neo4j.graphalgo.AlgorithmFactory;
import org.neo4j.graphalgo.WriteProc;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.write.RelationshipExporter;
import org.neo4j.graphalgo.nodesim.SimilarityGraphBuilder;
import org.neo4j.graphalgo.nodesim.SimilarityGraphResult;
import org.neo4j.graphalgo.result.AbstractResultBuilder;
import org.neo4j.graphalgo.results.MemoryEstimateResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.graphalgo.core.ProcedureConstants.HISTOGRAM_PRECISION_DEFAULT;
import static org.neo4j.graphalgo.knn.KnnProc.KNN_DESCRIPTION;
import static org.neo4j.graphalgo.nodesim.NodeSimilarityProc.shouldComputeHistogram;
import static org.neo4j.procedure.Mode.READ;
import static org.neo4j.procedure.Mode.WRITE;

public class KnnWriteProc extends WriteProc<Knn, Knn.Result, KnnWriteProc.WriteResult, KnnWriteConfig> {

    @Procedure(name = "gds.beta.knn.write", mode = WRITE)
    @Description(KNN_DESCRIPTION)
    public Stream<KnnWriteProc.WriteResult> write(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return write(compute(graphNameOrConfig, configuration));
    }

    @Procedure(value = "gds.beta.knn.write.estimate", mode = READ)
    @Description(ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimateWrite(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return computeEstimate(graphNameOrConfig, configuration);
    }

    @Override
    protected KnnWriteConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return KnnWriteConfig.of(username, graphName, maybeImplicitCreate, config);
    }

    @Override
    protected AbstractResultBuilder<WriteResult> resultBuilder(ComputationResult<Knn, Knn.Result, KnnWriteConfig> computeResult) {
        throw new UnsupportedOperationException("Knn handles result building individually.");
    }

    @Override
    protected AlgorithmFactory<Knn, KnnWriteConfig> algorithmFactory() {
        return new KnnFactory<>();
    }

    @Override
    protected Stream<WriteResult> write(ComputationResult<Knn, Knn.Result, KnnWriteConfig> computationResult) {
        return runWithExceptionLogging("Graph write failed", () -> {
            KnnWriteConfig config = computationResult.config();

            if (computationResult.isGraphEmpty()) {
                return Stream.of(
                    new KnnWriteProc.WriteResult(
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

            Knn algorithm = Objects.requireNonNull(computationResult.algorithm());
            SimilarityGraphResult similarityGraphResult = computeToGraph(
                computationResult.graph(),
                algorithm.nodeCount(),
                config.concurrency(),
                Objects.requireNonNull(computationResult.result()),
                algorithm.context()
            );

            Graph similarityGraph = similarityGraphResult.similarityGraph();

            KnnProc.KnnResultBuilder<KnnWriteProc.WriteResult> resultBuilder =
                KnnProc.resultBuilder(new KnnWriteProc.WriteResult.Builder(), computationResult, similarityGraphResult);

            if (similarityGraph.relationshipCount() > 0) {
                String writeRelationshipType = config.writeRelationshipType();
                String writeProperty = config.writeProperty();

                runWithExceptionLogging(
                    "Knn write-back failed",
                    () -> {
                        try (ProgressTimer ignored = ProgressTimer.start(resultBuilder::withWriteMillis)) {
                            RelationshipExporter exporter = RelationshipExporter
                                .of(api, similarityGraph, algorithm.getTerminationFlag())
                                .withLog(log)
                                .build();
                            if (shouldComputeHistogram(callContext)) {
                                DoubleHistogram histogram = new DoubleHistogram(HISTOGRAM_PRECISION_DEFAULT);
                                exporter.write(
                                    writeRelationshipType,
                                    Optional.of(writeProperty),
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
            }
            return Stream.of(resultBuilder.build());
        });
    }

    static SimilarityGraphResult computeToGraph(
        Graph graph,
        long nodeCount,
        int concurrency,
        Knn.Result result,
        KnnContext context
    ) {
        Graph similarityGraph = new SimilarityGraphBuilder(
            graph,
            concurrency,
            context.executor(),
            context.tracker()
        ).build(result.streamSimilarityResult());
        return new SimilarityGraphResult(similarityGraph, nodeCount, false);
    }

    public static class WriteResult {
        public final long createMillis;
        public final long computeMillis;
        public final long writeMillis;
        public final long postProcessingMillis;

        public final long nodesCompared;
        public final long relationshipsWritten;

        public final Map<String, Object> similarityDistribution;
        public final Map<String, Object> configuration;

        WriteResult(
            long createMillis,
            long computeMillis,
            long writeMillis,
            long postProcessingMillis,
            long nodesCompared,
            long relationshipsWritten,
            Map<String, Object> similarityDistribution,
            Map<String, Object> configuration
        ) {
            this.createMillis = createMillis;
            this.computeMillis = computeMillis;
            this.writeMillis = writeMillis;
            this.postProcessingMillis = postProcessingMillis;
            this.nodesCompared = nodesCompared;
            this.relationshipsWritten = relationshipsWritten;
            this.similarityDistribution = similarityDistribution;
            this.configuration = configuration;
        }

        static class Builder extends KnnProc.KnnResultBuilder<KnnWriteProc.WriteResult> {

            @Override
            public KnnWriteProc.WriteResult build() {
                return new KnnWriteProc.WriteResult(
                    createMillis,
                    computeMillis,
                    writeMillis,
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
