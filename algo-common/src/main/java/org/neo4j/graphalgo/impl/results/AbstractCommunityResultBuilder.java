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
package org.neo4j.graphalgo.impl.results;

import com.carrotsearch.hppc.cursors.LongLongCursor;
import org.HdrHistogram.Histogram;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongLongMap;

import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.LongUnaryOperator;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class AbstractCommunityResultBuilder<R> extends AbstractResultBuilder<R> {

    private static final Pattern PERCENTILE_FIELD_REGEXP = Pattern.compile("^p\\d{1,3}$");
    private static final Pattern COMMUNITY_COUNT_REGEXP = Pattern.compile("^(community|set)Count$");
    private static final long EXPECTED_NUMBER_OF_COMMUNITIES_DEFAULT = 4L;

    private final boolean buildHistogram;
    private final boolean buildCommunityCount;

    protected long postProcessingDuration = -1L;

    private LongUnaryOperator communityFunction = null;
    private OptionalLong maybeExpectedCommunityCount = OptionalLong.empty();

    protected OptionalLong maybeCommunityCount = OptionalLong.empty();
    protected Optional<Histogram> maybeCommunityHistogram = Optional.empty();

    private final AllocationTracker tracker;

    protected AbstractCommunityResultBuilder(Set<String> returnFields, AllocationTracker tracker) {
        this.buildHistogram = returnFields.stream().anyMatch(PERCENTILE_FIELD_REGEXP.asPredicate());
        this.buildCommunityCount = buildHistogram || returnFields
                .stream()
                .anyMatch(COMMUNITY_COUNT_REGEXP.asPredicate());
        this.tracker = tracker;
    }

    protected AbstractCommunityResultBuilder(Stream<String> returnFields, AllocationTracker tracker) {
        this(returnFields.collect(Collectors.toSet()), tracker);
    }

    public AbstractCommunityResultBuilder<R> withExpectedNumberOfCommunities(long expectedNumberOfCommunities) {
        this.maybeExpectedCommunityCount = OptionalLong.of(expectedNumberOfCommunities);
        return this;
    }

    public AbstractCommunityResultBuilder<R> withCommunityFunction(LongUnaryOperator communityFunction) {
        this.communityFunction = communityFunction;
        return this;
    }

    @Override
    public R build() {
        final ProgressTimer timer = ProgressTimer.start();

        if (buildCommunityCount && communityFunction != null) {
            long expectedNumberOfCommunities = maybeExpectedCommunityCount.orElse(EXPECTED_NUMBER_OF_COMMUNITIES_DEFAULT);
            HugeLongLongMap communitySizeMap = new HugeLongLongMap(expectedNumberOfCommunities, tracker);
            for (long nodeId = 0L; nodeId < nodeCount; nodeId++) {
                final long communityId = communityFunction.applyAsLong(nodeId);
                communitySizeMap.addTo(communityId, 1L);
            }

            if (buildHistogram) {
                maybeCommunityHistogram = Optional.of(buildFrom(communitySizeMap));
            }

            maybeCommunityCount = OptionalLong.of(communitySizeMap.size());
            communitySizeMap.release();
        }

        timer.stop();

        this.postProcessingDuration = timer.getDuration();

        return buildResult();
    }

    protected abstract R buildResult();

    static Histogram buildFrom(HugeLongLongMap communitySizeMap) {
        final Histogram histogram = new Histogram(5);

        for (LongLongCursor cursor : communitySizeMap) {
            histogram.recordValue(cursor.value);
        }

        return histogram;
    }

}
