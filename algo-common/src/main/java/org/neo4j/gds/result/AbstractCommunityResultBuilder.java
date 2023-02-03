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
package org.neo4j.gds.result;

import org.HdrHistogram.Histogram;
import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.api.ProcedureReturnColumns;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.ProgressTimer;

import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ExecutorService;
import java.util.function.LongUnaryOperator;

import static org.neo4j.gds.result.CommunityStatistics.communityCount;
import static org.neo4j.gds.result.CommunityStatistics.communityCountAndHistogram;

public abstract class AbstractCommunityResultBuilder<WRITE_RESULT> extends AbstractResultBuilder<WRITE_RESULT> {

    private final ExecutorService executorService;
    private final int concurrency;
    protected boolean buildHistogram;
    protected boolean buildCommunityCount;

    protected long postProcessingDuration = -1L;
    protected OptionalLong maybeCommunityCount = OptionalLong.empty();
    protected Optional<Histogram> maybeCommunityHistogram = Optional.empty();
    protected @Nullable Map<String, Object> communityHistogramOrNull() {
        return maybeCommunityHistogram.map(HistogramUtils::communitySummary).orElse(null);
    }

    private LongUnaryOperator communityFunction = null;

    protected AbstractCommunityResultBuilder(
        ProcedureReturnColumns returnColumns,
        int concurrency
    ) {
        this(returnColumns, Pools.DEFAULT, concurrency);
    }

    protected AbstractCommunityResultBuilder(
        ProcedureReturnColumns returnColumns,
        ExecutorService executorService,
        int concurrency
    ) {
        this.buildHistogram = returnColumns.contains("communityDistribution") || returnColumns.contains("componentDistribution");
        this.buildCommunityCount = returnColumns.contains("communityCount") || returnColumns.contains("componentCount");
        this.executorService = executorService;
        this.concurrency = concurrency;
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
                maybeCommunityCount = OptionalLong.of(communityCount(
                    nodeCount,
                    communityFunction,
                    executorService,
                    concurrency
                ));
            } else if (buildCommunityCount || buildHistogram) {
                var communityCountAndHistogram = communityCountAndHistogram(
                    nodeCount,
                    communityFunction,
                    executorService,
                    concurrency
                );
                maybeCommunityCount = OptionalLong.of(communityCountAndHistogram.componentCount());
                maybeCommunityHistogram = Optional.of(communityCountAndHistogram.histogram());
            }
        }

        timer.stop();

        this.postProcessingDuration = timer.getDuration();

        return buildResult();
    }

}
