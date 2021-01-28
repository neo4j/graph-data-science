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
package org.neo4j.graphalgo.result;

import org.HdrHistogram.Histogram;
import org.jetbrains.annotations.Nullable;
import org.neo4j.graphalgo.compat.MapUtil;
import org.neo4j.graphalgo.core.loading.SparseNodeMapping;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;

import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.LongUnaryOperator;

public abstract class AbstractCommunityResultBuilder<WRITE_RESULT> extends AbstractResultBuilder<WRITE_RESULT> {

    private final AllocationTracker tracker;
    protected boolean buildHistogram;
    protected boolean buildCommunityCount;

    protected long postProcessingDuration = -1L;
    protected OptionalLong maybeCommunityCount = OptionalLong.empty();
    protected Optional<Histogram> maybeCommunityHistogram = Optional.empty();
    protected @Nullable Map<String, Object> communityHistogramOrNull() {
        return maybeCommunityHistogram.map(histogram -> MapUtil.map(
            "min", histogram.getMinValue(),
            "mean", histogram.getMean(),
            "max", histogram.getMaxValue(),
            "p50", histogram.getValueAtPercentile(50),
            "p75", histogram.getValueAtPercentile(75),
            "p90", histogram.getValueAtPercentile(90),
            "p95", histogram.getValueAtPercentile(95),
            "p99", histogram.getValueAtPercentile(99),
            "p999", histogram.getValueAtPercentile(99.9)
        )).orElse(null);
    }

    private LongUnaryOperator communityFunction = null;

    protected AbstractCommunityResultBuilder(
        ProcedureCallContext callContext,
        AllocationTracker tracker
    ) {
        this.buildHistogram = callContext
            .outputFields()
            .anyMatch(s -> s.equalsIgnoreCase("communityDistribution") || s.equalsIgnoreCase("componentDistribution"));
        this.buildCommunityCount = callContext
            .outputFields()
            .anyMatch(s -> s.equalsIgnoreCase("communityCount") || s.equalsIgnoreCase("componentCount"));
        this.tracker = tracker;
    }

    protected abstract WRITE_RESULT buildResult();

    public AbstractCommunityResultBuilder<WRITE_RESULT> withCommunityFunction(LongUnaryOperator communityFunction) {
        this.communityFunction = communityFunction;
        return this;
    }

    @Override
    public WRITE_RESULT build() {
        final ProgressTimer timer = ProgressTimer.start();

        if (communityFunction != null) {
            if (buildCommunityCount && !buildHistogram) {
                buildCommunityCount();
            } else if (buildCommunityCount || buildHistogram){
                buildCommunityCountAndHistogram();
            }
        }

        timer.stop();

        this.postProcessingDuration = timer.getDuration();

        return buildResult();
    }

    private void buildCommunityCount() {
        long communityCount = 0L;

        SparseNodeMapping componentSizes = buildComponentSizes();
        for (long communityId = 0; communityId < componentSizes.getCapacity(); communityId++) {
            long communitySize = componentSizes.get(communityId);
            if (communitySize > 0) {
                communityCount++;
            }
        }

        maybeCommunityCount = OptionalLong.of(communityCount);
    }

    private void buildCommunityCountAndHistogram() {
        SparseNodeMapping componentSizes = buildComponentSizes();

        Histogram histogram = new Histogram(5);
        long communityCount = 0;
        for (long communityId = 0; communityId < componentSizes.getCapacity(); communityId++) {
            long communitySize = componentSizes.get(communityId);
            if (communitySize > 0) {
                communityCount++;
                histogram.recordValue(communitySize);
            }
        }

        maybeCommunityCount = OptionalLong.of(communityCount);
        maybeCommunityHistogram = Optional.of(histogram);
    }

    private SparseNodeMapping buildComponentSizes() {
        SparseNodeMapping.GrowingBuilder componentSizeBuilder =
            SparseNodeMapping.GrowingBuilder.create(0L, tracker);

        for (long nodeId = 0L; nodeId < nodeCount; nodeId++) {
            final long communityId = communityFunction.applyAsLong(nodeId);
            componentSizeBuilder.addTo(communityId, 1L);
        }
        return componentSizeBuilder.build();
    }

}
