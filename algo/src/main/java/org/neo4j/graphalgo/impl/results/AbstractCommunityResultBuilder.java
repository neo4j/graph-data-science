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
import java.util.function.IntUnaryOperator;
import java.util.function.LongToIntFunction;
import java.util.function.LongUnaryOperator;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author mknblch
 */
public abstract class AbstractCommunityResultBuilder<T> {

    public static final Pattern PERCENTILE_FIELD_REGEXP = Pattern.compile("^p\\d{1,3}$");
    public static final Pattern COMMUNITY_COUNT_REGEXP = Pattern.compile("^(community|set)Count$");
    protected long loadDuration = -1;
    protected long evalDuration = -1;
    protected long writeDuration = -1;
    protected boolean write = false;
    protected Set<String> returnFields;
    protected boolean buildHistogram;
    protected boolean buildCommunityCount;

    protected AbstractCommunityResultBuilder(Set<String> returnFields) {
        this.returnFields = returnFields;
        this.buildHistogram = returnFields.stream().anyMatch(PERCENTILE_FIELD_REGEXP.asPredicate());
        this.buildCommunityCount = buildHistogram || returnFields
                .stream()
                .anyMatch(COMMUNITY_COUNT_REGEXP.asPredicate());
    }

    protected AbstractCommunityResultBuilder(Stream<String> returnFields) {
        this(returnFields.collect(Collectors.toSet()));
    }

    public void setLoadDuration(long loadDuration) {
        this.loadDuration = loadDuration;
    }

    public void setEvalDuration(long evalDuration) {
        this.evalDuration = evalDuration;
    }

    public void setWriteDuration(long writeDuration) {
        this.writeDuration = writeDuration;
    }

    public AbstractCommunityResultBuilder<T> withWrite(boolean write) {
        this.write = write;
        return this;
    }

    /**
     * returns an AutoClosable which measures the time
     * until it gets closed. Saves the duration as loadMillis
     *
     * @return
     */
    public ProgressTimer timeLoad() {
        return ProgressTimer.start(this::setLoadDuration);
    }

    /**
     * returns an AutoClosable which measures the time
     * until it gets closed. Saves the duration as evalMillis
     *
     * @return
     */
    public ProgressTimer timeEval() {
        return ProgressTimer.start(this::setEvalDuration);
    }

    /**
     * returns an AutoClosable which measures the time
     * until it gets closed. Saves the duration as writeMillis
     *
     * @return
     */
    public ProgressTimer timeWrite() {
        return ProgressTimer.start(this::setWriteDuration);
    }

    /**
     * evaluates loadMillis
     *
     * @param runnable
     */
    public void timeLoad(Runnable runnable) {
        try (ProgressTimer timer = timeLoad()) {
            runnable.run();
        }
    }

    /**
     * evaluates comuteMillis
     *
     * @param runnable
     */
    public void timeEval(Runnable runnable) {
        try (ProgressTimer timer = timeEval()) {
            runnable.run();
        }
    }

    public <U> U timeEval(Supplier<U> supplier) {
        try (ProgressTimer timer = timeEval()) {
            return supplier.get();
        }
    }

    /**
     * evaluates writeMillis
     *
     * @param runnable
     */
    public void timeWrite(Runnable runnable) {
        try (ProgressTimer timer = timeWrite()) {
            runnable.run();
        }
    }

    public T buildfromKnownSizes(int nodeCount, IntUnaryOperator sizeForNode) {
        return buildfromKnownLongSizes(
                nodeCount,
                (nodeId) -> sizeForNode.applyAsInt(Math.toIntExact(nodeId)));
    }

    public T buildfromKnownLongSizes(long nodeCount, LongToIntFunction sizeForNode) {
        final ProgressTimer timer = ProgressTimer.start();

        Optional<Histogram> maybeHistorgram = Optional.empty();
        if (buildHistogram) {
            final Histogram histogram = new Histogram(5);
            for (long nodeId = 0L; nodeId < nodeCount; nodeId++) {
                final int communitySize = sizeForNode.applyAsInt(nodeId);
                histogram.recordValue(communitySize);
            }

            maybeHistorgram = Optional.of(histogram);
        }

        timer.stop();

        return build(
                loadDuration,
                evalDuration,
                writeDuration,
                timer.getDuration(),
                nodeCount,
                OptionalLong.of(nodeCount),
                maybeHistorgram,
                write
        );
    }

    /**
     * build result
     */
    public T build(
            AllocationTracker tracker,
            long nodeCount,
            LongUnaryOperator fun) {
        return build(4L, tracker, nodeCount, fun);
    }

    /**
     * build result. If you know (or can reasonably estimate) the number of final communities,
     * prefer this overload over {@link #build(AllocationTracker, long, LongUnaryOperator)} as this
     * one will presize the counting bag and avoid excessive allocations and resizing operations.
     */
    public T build(
            long expectedNumberOfCommunities,
            AllocationTracker tracker,
            long nodeCount,
            LongUnaryOperator fun) {

        final ProgressTimer timer = ProgressTimer.start();

        OptionalLong maybeCommunityCount = OptionalLong.empty();
        Optional<Histogram> maybeHistogram = Optional.empty();

        if (buildCommunityCount) {
            HugeLongLongMap communitySizeMap = new HugeLongLongMap(expectedNumberOfCommunities, tracker);
            for (long nodeId = 0L; nodeId < nodeCount; nodeId++) {
                final long communityId = fun.applyAsLong(nodeId);
                communitySizeMap.addTo(communityId, 1L);
            }

            if (buildHistogram) {
                maybeHistogram = Optional.of(buildFrom(communitySizeMap));
            }

            maybeCommunityCount = OptionalLong.of(communitySizeMap.size());
            communitySizeMap.release();
        }

        timer.stop();

        return build(
                loadDuration,
                evalDuration,
                writeDuration,
                timer.getDuration(),
                nodeCount,
                maybeCommunityCount,
                maybeHistogram,
                write
        );
    }

    protected abstract T build(
            long loadMillis,
            long computeMillis,
            long writeMillis,
            long postProcessingMillis,
            long nodeCount,
            OptionalLong maybeCommunityCount,
            Optional<Histogram> maybeCommunityHistogram,
            boolean write);


    static Histogram buildFrom(HugeLongLongMap communitySizeMap) {
        final Histogram histogram = new Histogram(5);

        for (LongLongCursor cursor : communitySizeMap) {
            histogram.recordValue(cursor.value);
        }

        return histogram;
    }

}
