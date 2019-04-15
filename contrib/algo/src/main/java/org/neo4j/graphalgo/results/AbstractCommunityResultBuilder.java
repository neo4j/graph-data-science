/*
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
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
package org.neo4j.graphalgo.results;

import com.carrotsearch.hppc.LongLongMap;
import com.carrotsearch.hppc.LongLongScatterMap;
import org.HdrHistogram.Histogram;
import org.neo4j.graphalgo.core.utils.ProgressTimer;

import java.util.function.IntFunction;
import java.util.function.LongToIntFunction;
import java.util.function.LongUnaryOperator;

/**
 * @author mknblch
 */
public abstract class AbstractCommunityResultBuilder<T> {

    protected long loadDuration = -1;
    protected long evalDuration = -1;
    protected long writeDuration = -1;
    protected boolean write = false;

    public AbstractCommunityResultBuilder<T> withLoadDuration(long loadDuration) {
        this.loadDuration = loadDuration;
        return this;
    }


    public AbstractCommunityResultBuilder<T> withEvalDuration(long evalDuration) {
        this.evalDuration = evalDuration;
        return this;
    }


    public AbstractCommunityResultBuilder<T> withWriteDuration(long writeDuration) {
        this.writeDuration = writeDuration;
        return this;
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
        return ProgressTimer.start(this::withLoadDuration);
    }

    /**
     * returns an AutoClosable which measures the time
     * until it gets closed. Saves the duration as evalMillis
     *
     * @return
     */
    public ProgressTimer timeEval() {
        return ProgressTimer.start(this::withEvalDuration);
    }

    /**
     * returns an AutoClosable which measures the time
     * until it gets closed. Saves the duration as writeMillis
     *
     * @return
     */
    public ProgressTimer timeWrite() {
        return ProgressTimer.start(this::withWriteDuration);
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

    public T buildII(long nodeCount, IntFunction<Integer> fun) {
        final ProgressTimer timer = ProgressTimer.start();
        final Histogram histogram = new Histogram(2);
        for (int nodeId = 0; nodeId < nodeCount; nodeId++) {
            final long size = fun.apply(nodeId);
            histogram.recordValue(size);
        }

        timer.stop();

        final LongLongMap communitySizeMap = new LongLongScatterMap();
        return build(loadDuration,
                evalDuration,
                writeDuration,
                timer.getDuration(),
                nodeCount,
                communitySizeMap.size(),
                communitySizeMap,
                histogram,
                write
        );


    }

    public T buildLI(long nodeCount, LongToIntFunction fun) {

        final Histogram histogram = new Histogram(2);
        final ProgressTimer timer = ProgressTimer.start();
        for (int nodeId = 0; nodeId < nodeCount; nodeId++) {
            final long size = fun.applyAsInt(nodeId);
            histogram.recordValue(size);
        }

        timer.stop();

        final LongLongMap communitySizeMap = new LongLongScatterMap();
        return build(loadDuration,
                evalDuration,
                writeDuration,
                timer.getDuration(),
                nodeCount,
                communitySizeMap.size(),
                communitySizeMap,
                histogram,
                write
        );
    }

    /**
     * build result
     */
    public T build(long nodeCount, LongUnaryOperator fun) {

        final LongLongMap communitySizeMap = new LongLongScatterMap();
        final ProgressTimer timer = ProgressTimer.start();
        for (long nodeId = 0L; nodeId < nodeCount; nodeId++) {
            final long communityId = fun.applyAsLong(nodeId);
            communitySizeMap.addTo(communityId, 1L);
        }

        Histogram histogram = CommunityHistogram.buildFrom(communitySizeMap);

        timer.stop();

        return build(loadDuration,
                evalDuration,
                writeDuration,
                timer.getDuration(),
                nodeCount,
                communitySizeMap.size(),
                communitySizeMap,
                histogram,
                write
        );
    }

    protected abstract T build(
            long loadMillis,
            long computeMillis,
            long writeMillis,
            long postProcessingMillis,
            long nodeCount,
            long communityCount,
            LongLongMap communitySizeMap,
            Histogram communityHistogram,
            boolean write);

}
