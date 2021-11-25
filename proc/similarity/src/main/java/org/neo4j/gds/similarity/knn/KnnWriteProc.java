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
package org.neo4j.gds.similarity.knn;

import org.neo4j.gds.AlgorithmFactory;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.config.GraphCreateConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.gds.similarity.SimilarityGraphBuilder;
import org.neo4j.gds.similarity.SimilarityGraphResult;
import org.neo4j.gds.similarity.SimilarityProc;
import org.neo4j.gds.similarity.SimilarityWriteProc;
import org.neo4j.gds.similarity.SimilarityWriteResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.gds.similarity.knn.KnnProc.KNN_DESCRIPTION;
import static org.neo4j.procedure.Mode.READ;
import static org.neo4j.procedure.Mode.WRITE;

public class KnnWriteProc extends SimilarityWriteProc<Knn, Knn.Result, KnnWriteProc.Result, KnnWriteConfig> {

    @Procedure(name = "gds.beta.knn.write", mode = WRITE)
    @Description(KNN_DESCRIPTION)
    public Stream<Result> write(
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
    public String procedureName() {
        return "KNN";
    }

    @Override
    protected SimilarityProc.SimilarityResultBuilder<Result> resultBuilder(ComputationResult<Knn, Knn.Result, KnnWriteConfig> computationResult) {
        if (computationResult.isGraphEmpty()) {
            return new Result.Builder();
        }

        return new Result.Builder()
            .withDidConverge(computationResult.result().didConverge())
            .withNodePairsConsidered(computationResult.result().nodePairsConsidered())
            .withRanIterations(computationResult.result().ranIterations());
    }

    @Override
    protected KnnWriteConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return KnnWriteConfig.of(graphName, maybeImplicitCreate, config);
    }

    @Override
    protected AlgorithmFactory<Knn, KnnWriteConfig> algorithmFactory() {
        return new KnnFactory<>();
    }

    @Override
    protected SimilarityGraphResult similarityGraphResult(ComputationResult<Knn, Knn.Result, KnnWriteConfig> computationResult) {
        Knn algorithm = Objects.requireNonNull(computationResult.algorithm());
        KnnWriteConfig config = computationResult.config();
        return computeToGraph(
            computationResult.graph(),
            algorithm.nodeCount(),
            config.concurrency(),
            Objects.requireNonNull(computationResult.result()),
            algorithm.context()
        );
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
            context.allocationTracker()
        ).build(result.streamSimilarityResult());
        return new SimilarityGraphResult(similarityGraph, nodeCount, false);
    }

    public static class Result extends SimilarityWriteResult {
        public final long ranIterations;
        public final boolean didConverge;
        public final long nodePairsConsidered;

        Result(
            long createMillis,
            long computeMillis,
            long writeMillis,
            long postProcessingMillis,
            long nodesCompared,
            long relationshipsWritten,
            boolean didConverge,
            long ranIterations,
            long nodePairsCompared,
            Map<String, Object> similarityDistribution,
            Map<String, Object> configuration
        ) {
            super(
                createMillis,
                computeMillis,
                writeMillis,
                postProcessingMillis,
                nodesCompared,
                relationshipsWritten,
                similarityDistribution,
                configuration
            );

            this.nodePairsConsidered = nodePairsCompared;
            this.ranIterations = ranIterations;
            this.didConverge = didConverge;
        }

        @SuppressWarnings("unused")
        static class Builder extends SimilarityProc.SimilarityResultBuilder<Result> {
            public long ranIterations;
            public boolean didConverge;
            public long nodePairsConsidered;

            @Override
            public Result build() {
                return new Result(
                    createMillis,
                    computeMillis,
                    writeMillis,
                    postProcessingMillis,
                    nodesCompared,
                    relationshipsWritten,
                    didConverge,
                    ranIterations,
                    nodePairsConsidered,
                    distribution(),
                    config.toMap()
                );
            }

            public Builder withDidConverge(boolean didConverge) {
                this.didConverge = didConverge;
                return this;
            }

            public Builder withRanIterations(long ranIterations) {
                this.ranIterations = ranIterations;
                return this;
            }

            Builder withNodePairsConsidered(long nodePairsConsidered) {
                this.nodePairsConsidered = nodePairsConsidered;
                return this;
            }
        }
    }
}
