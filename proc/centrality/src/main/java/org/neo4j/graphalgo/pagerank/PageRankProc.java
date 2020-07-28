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
package org.neo4j.graphalgo.pagerank;

import org.HdrHistogram.DoubleHistogram;
import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.api.nodeproperties.DoubleNodeProperties;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.result.AbstractResultBuilder;
import org.neo4j.internal.helpers.collection.MapUtil;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.function.LongToDoubleFunction;

import static org.neo4j.graphalgo.core.ProcedureConstants.HISTOGRAM_PRECISION_DEFAULT;

final class PageRankProc {

    static final String PAGE_RANK_DESCRIPTION =
        "Page Rank is an algorithm that measures the transitive influence or connectivity of nodes.";

    private PageRankProc() {}

    static <PROC_RESULT, CONFIG extends PageRankBaseConfig> PageRankResultBuilder<PROC_RESULT> resultBuilder(
        PageRankResultBuilder<PROC_RESULT> procResultBuilder,
        AlgoBaseProc.ComputationResult<PageRank, PageRank, CONFIG> computeResult
    ) {
        PageRank result = computeResult.result();
        procResultBuilder
            .withDidConverge(!computeResult.isGraphEmpty() && result.didConverge())
            .withRanIterations(!computeResult.isGraphEmpty() ? result.iterations() : 0)
            .withCentralityFunction(!computeResult.isGraphEmpty() ? computeResult.result().result()::score : null)
            .withCreateMillis(computeResult.createMillis())
            .withComputeMillis(computeResult.computeMillis())
            .withConfig(computeResult.config());

        return procResultBuilder;
    }

    static <CONFIG extends PageRankBaseConfig> NodeProperties nodeProperties(AlgoBaseProc.ComputationResult<PageRank, PageRank, CONFIG> computeResult) {
        return (DoubleNodeProperties) computeResult.result().result()::score;
    }

    abstract static class PageRankResultBuilder<PROC_RESULT> extends AbstractResultBuilder<PROC_RESULT> {

        private final boolean buildHistogram;

        protected long ranIterations;

        protected boolean didConverge;

        long postProcessingMillis = -1L;

        Optional<DoubleHistogram> maybeHistogram = Optional.empty();

        LongToDoubleFunction centralityFunction = null;

        protected PageRankResultBuilder(ProcedureCallContext callContext) {
            this.buildHistogram = callContext
                .outputFields()
                .anyMatch(s -> s.equalsIgnoreCase("centralityDistribution"));
        }

        protected abstract PROC_RESULT buildResult();

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

        PageRankResultBuilder<PROC_RESULT> withRanIterations(long ranIterations) {
            this.ranIterations = ranIterations;
            return this;
        }

        PageRankResultBuilder<PROC_RESULT> withDidConverge(boolean didConverge) {
            this.didConverge = didConverge;
            return this;
        }

        PageRankResultBuilder<PROC_RESULT> withCentralityFunction(LongToDoubleFunction centralityFunction) {
            this.centralityFunction = centralityFunction;
            return this;
        }

        ProgressTimer timePostProcessing() {
            return ProgressTimer.start(this::setPostProcessingMillis);
        }

        void setPostProcessingMillis(long postProcessingMillis) {
            this.postProcessingMillis = postProcessingMillis;
        }

        @Override
        public PROC_RESULT build() {
            ProgressTimer timer = ProgressTimer.start();

            if (centralityFunction != null && buildHistogram) {
                DoubleHistogram histogram = new DoubleHistogram(HISTOGRAM_PRECISION_DEFAULT);
                for (long nodeId = 0; nodeId < nodeCount; nodeId++) {
                    double centralityValue = centralityFunction.applyAsDouble(nodeId);
                    histogram.recordValue(centralityValue);
                }
                maybeHistogram = Optional.of(histogram);
            }

            timer.stop();

            this.postProcessingMillis = timer.getDuration();

            return buildResult();
        }
    }
}
