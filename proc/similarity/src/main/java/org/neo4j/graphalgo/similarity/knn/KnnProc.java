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
package org.neo4j.graphalgo.similarity.knn;

import org.HdrHistogram.DoubleHistogram;
import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.compat.MapUtil;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.result.AbstractResultBuilder;
import org.neo4j.graphalgo.similarity.SimilarityGraphResult;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

final class KnnProc {

    public static final String KNN_DESCRIPTION =
        "The k-nearest neighbor graph algorithm constructs relationships between nodes if the distance " +
        "between two nodes is among the k nearest distances compared to other nodes." +
        "KNN computes distances based on the similarity of node properties";

    private KnnProc() {}

    static <PROC_RESULT, CONFIG extends KnnBaseConfig> KnnResultBuilder<PROC_RESULT> resultBuilder(
        KnnResultBuilder<PROC_RESULT> procResultBuilder,
        AlgoBaseProc.ComputationResult<Knn, Knn.Result, CONFIG> computationResult,
        SimilarityGraphResult similarityGraphResult
    ) {
        resultBuilder(procResultBuilder, computationResult)
            .withNodesCompared(similarityGraphResult.comparedNodes())
            .withRelationshipsWritten(similarityGraphResult.similarityGraph().relationshipCount());

        return procResultBuilder;
    }

    static <PROC_RESULT, CONFIG extends KnnBaseConfig> KnnResultBuilder<PROC_RESULT> resultBuilder(
        KnnResultBuilder<PROC_RESULT> procResultBuilder,
        AlgoBaseProc.ComputationResult<Knn, Knn.Result, CONFIG> computationResult
    ) {
        procResultBuilder
            .withCreateMillis(computationResult.createMillis())
            .withComputeMillis(computationResult.computeMillis())
            .withConfig(computationResult.config());

        return procResultBuilder;
    }

    abstract static class KnnResultBuilder<PROC_RESULT> extends AbstractResultBuilder<PROC_RESULT> {
        long nodesCompared = 0L;

        long postProcessingMillis = -1L;

        Optional<DoubleHistogram> maybeHistogram = Optional.empty();

        Map<String, Object> distribution() {
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

        KnnProc.KnnResultBuilder<PROC_RESULT> withNodesCompared(long nodesCompared) {
            this.nodesCompared = nodesCompared;
            return this;
        }

        KnnProc.KnnResultBuilder<PROC_RESULT> withHistogram(DoubleHistogram histogram) {
            this.maybeHistogram = Optional.of(histogram);
            return this;
        }

        ProgressTimer timePostProcessing() {
            return ProgressTimer.start(this::setPostProcessingMillis);
        }

        void setPostProcessingMillis(long postProcessingMillis) {
            this.postProcessingMillis = postProcessingMillis;
        }
    }
}
