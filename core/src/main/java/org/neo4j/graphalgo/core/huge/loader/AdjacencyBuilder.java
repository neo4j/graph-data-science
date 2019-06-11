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
package org.neo4j.graphalgo.core.huge.loader;

import org.apache.lucene.util.LongsRef;
import org.neo4j.graphalgo.core.huge.HugeAdjacencyOffsets;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicLong;

import static org.neo4j.graphalgo.core.utils.paged.MemoryUsage.sizeOfLongArray;
import static org.neo4j.graphalgo.core.utils.paged.MemoryUsage.sizeOfObjectArray;

abstract class AdjacencyBuilder {

    abstract void addAdjacencyImporter(
            AllocationTracker tracker,
            boolean loadDegrees,
            int pageIndex);

    abstract void finishPreparation();

    abstract void addAll(
            long[] batch,
            long[] targets,
            int[] offsets,
            int length,
            AllocationTracker tracker);

    abstract Collection<Runnable> flushTasks();

    static AdjacencyBuilder compressing(
            HugeAdjacencyBuilder adjacency,
            int numPages,
            int pageSize,
            AllocationTracker tracker,
            AtomicLong relationshipCounter) {
        if (adjacency == null) {
            return NoAdjacency.INSTANCE;
        }
        tracker.add(sizeOfObjectArray(numPages) << 2);
        HugeAdjacencyBuilder[] builders = new HugeAdjacencyBuilder[numPages];
        final CompressedLongArray[][] targets = new CompressedLongArray[numPages][];
        LongsRef[] buffers = new LongsRef[numPages];
        long[][] degrees = new long[numPages][];
        return new CompressingPagedAdjacency(adjacency, builders, targets, buffers, degrees, pageSize, relationshipCounter);
    }

    private static final class CompressingPagedAdjacency extends AdjacencyBuilder {

        private final HugeAdjacencyBuilder adjacency;
        private final HugeAdjacencyBuilder[] builders;
        private final CompressedLongArray[][] targets;
        private final LongsRef[] buffers;
        private final long[][] degrees;
        private final int pageSize;
        private final int pageShift;
        private final long pageMask;
        private final long sizeOfLongPage;
        private final long sizeOfObjectPage;
        private final AtomicLong relationshipCounter;

        private CompressingPagedAdjacency(
                HugeAdjacencyBuilder adjacency,
                HugeAdjacencyBuilder[] builders,
                CompressedLongArray[][] targets,
                LongsRef[] buffers,
                long[][] degrees,
                int pageSize,
                AtomicLong relationshipCounter) {
            this.adjacency = adjacency;
            this.builders = builders;
            this.targets = targets;
            this.buffers = buffers;
            this.degrees = degrees;
            this.pageSize = pageSize;
            this.pageShift = Integer.numberOfTrailingZeros(pageSize);
            this.pageMask = (long) (pageSize - 1);
            sizeOfLongPage = sizeOfLongArray(pageSize);
            sizeOfObjectPage = sizeOfObjectArray(pageSize);
            this.relationshipCounter = relationshipCounter;
        }

        @Override
        void addAdjacencyImporter(AllocationTracker tracker, boolean loadDegrees, int pageIndex) {
            tracker.add(sizeOfObjectPage);
            tracker.add(sizeOfObjectPage);
            tracker.add(sizeOfLongPage);
            targets[pageIndex] = new CompressedLongArray[pageSize];
            buffers[pageIndex] = new LongsRef();
            long[] offsets = degrees[pageIndex] = new long[pageSize];
            builders[pageIndex] = adjacency.threadLocalCopy(offsets, loadDegrees);
            builders[pageIndex].prepare();
        }

        @Override
        void finishPreparation() {
            adjacency.setGlobalOffsets(HugeAdjacencyOffsets.of(degrees, pageSize));
        }

        @Override
        void addAll(
                long[] batch,
                long[] targets,
                int[] offsets,
                int length,
                AllocationTracker tracker) {
            int pageShift = this.pageShift;
            long pageMask = this.pageMask;

            HugeAdjacencyBuilder builder = null;
            int lastPageIndex = -1;
            int endOffset, startOffset = 0;
            try {
                for (int i = 0; i < length; ++i) {
                    endOffset = offsets[i];

                    if (endOffset <= startOffset) {
                        continue;
                    }

                    long source = batch[startOffset << 2];
                    int pageIndex = (int) (source >>> pageShift);

                    if (pageIndex > lastPageIndex) {
                        if (builder != null) {
                            builder.unlock();
                        }
                        builder = builders[pageIndex];
                        builder.lock();
                        lastPageIndex = pageIndex;
                    }

                    int localId = (int) (source & pageMask);

                    int degree = builder.degree(localId);
                    CompressedLongArray compressedTargets = this.targets[pageIndex][localId];

                    if (compressedTargets == null) {
                        compressedTargets = new CompressedLongArray(tracker, degree);
                        this.targets[pageIndex][localId] = compressedTargets;
                    }

                    compressedTargets.addDeltas(targets, startOffset, endOffset);
                    int currentDegree = compressedTargets.length();
                    if (currentDegree >= degree) {
                        int importedRelationships = builder.applyVariableDeltaEncoding(
                                compressedTargets,
                                this.buffers[pageIndex],
                                localId);
                        relationshipCounter.addAndGet(importedRelationships);
                        this.targets[pageIndex][localId] = null;
                    }

                    startOffset = endOffset;
                }
            } finally {
                if (builder != null) {
                    builder.unlock();
                }
            }
        }

        @Override
        Collection<Runnable> flushTasks() {
            Runnable[] runnables = new Runnable[builders.length];
            Arrays.setAll(runnables, index -> () -> {
                HugeAdjacencyBuilder builder = builders[index];
                CompressedLongArray[] allTargets = targets[index];
                LongsRef buffer = buffers[index];
                long importedRelationships = 0L;
                for (int localId = 0; localId < allTargets.length; ++localId) {
                    CompressedLongArray target = allTargets[localId];
                    if (target != null) {
                        importedRelationships += builder.applyVariableDeltaEncoding(target, buffer, localId);
                        allTargets[localId] = null;
                    }
                }
                relationshipCounter.addAndGet(importedRelationships);
            });
            return Arrays.asList(runnables);
        }
    }

    private static final class NoAdjacency extends AdjacencyBuilder {

        private static final AdjacencyBuilder INSTANCE = new NoAdjacency();

        @Override
        void addAdjacencyImporter(
                AllocationTracker tracker,
                boolean loadDegrees,
                int pageIndex) {
        }

        @Override
        void finishPreparation() {
        }

        @Override
        void addAll(long[] batch, long[] targets, int[] offsets, int length, AllocationTracker tracker) {
        }

        @Override
        Collection<Runnable> flushTasks() {
            return Collections.emptyList();
        }
    }
}
