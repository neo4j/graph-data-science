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
package org.neo4j.graphalgo.similarity;

import org.HdrHistogram.DoubleHistogram;
import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.compat.MapUtil;
import org.neo4j.graphalgo.config.AlgoBaseConfig;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.result.AbstractResultBuilder;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static org.neo4j.graphalgo.core.ProcedureConstants.HISTOGRAM_PRECISION_DEFAULT;

public final class SimilarityProc {

    private SimilarityProc() {}

    public static boolean shouldComputeHistogram(ProcedureCallContext callContext) {
        return callContext
            .outputFields()
            .anyMatch(s -> s.equalsIgnoreCase("similarityDistribution"));
    }

    public static <RESULT, PROC_RESULT, CONFIG extends AlgoBaseConfig> SimilarityResultBuilder<PROC_RESULT> resultBuilder(
        SimilarityResultBuilder<PROC_RESULT> procResultBuilder,
        AlgoBaseProc.ComputationResult<? extends Algorithm<?, ?>, RESULT, CONFIG> computationResult,
        Function<RESULT, SimilarityGraphResult> graphResultFunc
    ) {
        RESULT result = computationResult.result();
        SimilarityGraphResult graphResult = graphResultFunc.apply(result);
        resultBuilder(procResultBuilder, computationResult)
            .withNodesCompared(graphResult.comparedNodes())
            .withRelationshipsWritten(graphResult.similarityGraph().relationshipCount());

        return procResultBuilder;
    }

    public static <PROC_RESULT, CONFIG extends AlgoBaseConfig> SimilarityResultBuilder<PROC_RESULT> resultBuilder(
        SimilarityResultBuilder<PROC_RESULT> procResultBuilder,
        AlgoBaseProc.ComputationResult<? extends Algorithm<?, ?>, ?, CONFIG> computationResult
    ) {
        procResultBuilder
            .withCreateMillis(computationResult.createMillis())
            .withComputeMillis(computationResult.computeMillis())
            .withConfig(computationResult.config());

        return procResultBuilder;
    }

    public static DoubleHistogram computeHistogram(Graph similarityGraph) {
        DoubleHistogram histogram = new DoubleHistogram(HISTOGRAM_PRECISION_DEFAULT);
        similarityGraph.forEachNode(nodeId -> {
            similarityGraph.forEachRelationship(nodeId, Double.NaN, (node1, node2, property) -> {
                histogram.recordValue(property);
                return true;
            });
            return true;
        });
        return histogram;
    }

    public abstract static class SimilarityResultBuilder<PROC_RESULT> extends AbstractResultBuilder<PROC_RESULT> {

        public long nodesCompared = 0L;

        public long postProcessingMillis = -1L;

        Optional<DoubleHistogram> maybeHistogram = Optional.empty();

        public Map<String, Object> distribution() {
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

        public SimilarityResultBuilder<PROC_RESULT> withNodesCompared(long nodesCompared) {
            this.nodesCompared = nodesCompared;
            return this;
        }

        public SimilarityResultBuilder<PROC_RESULT> withHistogram(DoubleHistogram histogram) {
            this.maybeHistogram = Optional.of(histogram);
            return this;
        }

        public ProgressTimer timePostProcessing() {
            return ProgressTimer.start(this::setPostProcessingMillis);
        }

        public void setPostProcessingMillis(long postProcessingMillis) {
            this.postProcessingMillis = postProcessingMillis;
        }
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

        public WriteResult(
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

        public static class Builder extends SimilarityProc.SimilarityResultBuilder<SimilarityProc.WriteResult> {

            @Override
            public SimilarityProc.WriteResult build() {
                return new SimilarityProc.WriteResult(
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

    public static class MutateResult {
        public final long createMillis;
        public final long computeMillis;
        public final long mutateMillis;
        public final long postProcessingMillis;

        public final long nodesCompared;
        public final long relationshipsWritten;

        public final Map<String, Object> similarityDistribution;
        public final Map<String, Object> configuration;

        public MutateResult(
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

        public static class Builder extends SimilarityProc.SimilarityResultBuilder<MutateResult> {

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

    public static final class StatsResult {

        public long createMillis;
        public long computeMillis;
        public long postProcessingMillis;

        public long nodesCompared;
        public long similarityPairs;
        public Map<String, Object> similarityDistribution;
        public Map<String, Object> configuration;

        public StatsResult(
            long createMillis,
            long computeMillis,
            long postProcessingMillis,
            long nodesCompared,
            long similarityPairs,
            Map<String, Object> communityDistribution,
            Map<String, Object> configuration

        ) {
            this.createMillis = createMillis;
            this.computeMillis = computeMillis;
            this.postProcessingMillis = postProcessingMillis;
            this.nodesCompared = nodesCompared;
            this.similarityPairs = similarityPairs;
            this.similarityDistribution = communityDistribution;
            this.configuration = configuration;
        }

        public static class Builder extends SimilarityProc.SimilarityResultBuilder<StatsResult> {

            @Override
            public SimilarityProc.StatsResult build() {
                return new SimilarityProc.StatsResult(
                    createMillis,
                    computeMillis,
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
