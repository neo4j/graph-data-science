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
package org.neo4j.graphalgo.core.loading;

import org.apache.lucene.util.LongsRef;
import org.neo4j.graphalgo.core.huge.AdjacencyOffsets;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.atomic.LongAdder;

import static org.neo4j.graphalgo.compat.StatementConstantsProxy.NO_SUCH_PROPERTY_KEY;
import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.sizeOfLongArray;
import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.sizeOfObjectArray;

public abstract class AdjacencyBuilder {

    abstract void addAll(
            long[] batch,
            long[] targets,
            long[][] propertyValues,
            int[] offsets,
            int length,
            AllocationTracker tracker);

    abstract Collection<Runnable> flushTasks();

    public static AdjacencyBuilder compressing(
            RelationshipsBuilder globalBuilder,
            int numPages,
            int pageSize,
            AllocationTracker tracker,
            LongAdder relationshipCounter,
            int[] propertyKeyIds,
            double[] defaultValues) {
        if (globalBuilder == null) {
            return NoAdjacency.INSTANCE;
        }
        tracker.add(sizeOfObjectArray(numPages) << 2);
        ThreadLocalRelationshipsBuilder[] localBuilders = new ThreadLocalRelationshipsBuilder[numPages];
        final CompressedLongArray[][] compressedAdjacencyLists = new CompressedLongArray[numPages][];
        LongsRef[] buffers = new LongsRef[numPages];
        long[][] globalAdjacencyOffsets = new long[numPages][];

        long[][][] globalWeightOffsets = new long[propertyKeyIds.length][][];
        Arrays.setAll(globalWeightOffsets, i -> new long[numPages][]);

        boolean atLeastOnePropertyToLoad = Arrays
                .stream(propertyKeyIds)
                .anyMatch(keyId -> keyId != NO_SUCH_PROPERTY_KEY);

        CompressingPagedAdjacency compressingPagedAdjacency = new CompressingPagedAdjacency(
            globalBuilder,
            localBuilders,
            compressedAdjacencyLists,
            buffers,
            globalAdjacencyOffsets,
            globalWeightOffsets,
            pageSize,
            relationshipCounter,
            propertyKeyIds,
            defaultValues,
            atLeastOnePropertyToLoad
        );
        for (int idx = 0; idx < numPages; idx++) {
            compressingPagedAdjacency.addAdjacencyImporter(tracker, idx);
        }
        compressingPagedAdjacency.finishPreparation();
        return compressingPagedAdjacency;
    }

    abstract int[] getPropertyKeyIds();

    abstract double[] getDefaultValues();

    abstract boolean atLeastOnePropertyToLoad();

    private static final class CompressingPagedAdjacency extends AdjacencyBuilder {

        private final RelationshipsBuilder globalBuilder;
        private final ThreadLocalRelationshipsBuilder[] localBuilders;
        private final CompressedLongArray[][] compressedAdjacencyLists;
        private final LongsRef[] buffers;
        private final long[][] globalAdjacencyOffsets;
        private final long[][][] globalWeightOffsets;
        private final int pageSize;
        private final int pageShift;
        private final long pageMask;
        private final long sizeOfLongPage;
        private final long sizeOfObjectPage;
        private final LongAdder relationshipCounter;
        private final int[] propertyKeyIds;
        private final double[] defaultValues;
        private final boolean atLeastOnePropertyToLoad;

        private CompressingPagedAdjacency(
            RelationshipsBuilder globalBuilder,
            ThreadLocalRelationshipsBuilder[] localBuilders,
            CompressedLongArray[][] compressedAdjacencyLists,
            LongsRef[] buffers,
            long[][] globalAdjacencyOffsets,
            long[][][] globalWeightOffsets,
            int pageSize,
            LongAdder relationshipCounter,
            int[] propertyKeyIds,
            double[] defaultValues,
            boolean atLeastOnePropertyToLoad
        ) {
            this.globalBuilder = globalBuilder;
            this.localBuilders = localBuilders;
            this.compressedAdjacencyLists = compressedAdjacencyLists;
            this.buffers = buffers;
            this.globalAdjacencyOffsets = globalAdjacencyOffsets;
            this.globalWeightOffsets = globalWeightOffsets;
            this.pageSize = pageSize;
            this.pageShift = Integer.numberOfTrailingZeros(pageSize);
            this.pageMask = pageSize - 1;
            this.sizeOfLongPage = sizeOfLongArray(pageSize);
            this.sizeOfObjectPage = sizeOfObjectArray(pageSize);
            this.relationshipCounter = relationshipCounter;
            this.propertyKeyIds = propertyKeyIds;
            this.defaultValues = defaultValues;
            this.atLeastOnePropertyToLoad = atLeastOnePropertyToLoad;
        }

        void addAdjacencyImporter(AllocationTracker tracker, int pageIndex) {
            tracker.add(sizeOfObjectPage);
            tracker.add(sizeOfObjectPage);
            tracker.add(sizeOfLongPage);
            compressedAdjacencyLists[pageIndex] = new CompressedLongArray[pageSize];
            buffers[pageIndex] = new LongsRef();
            long[] localAdjacencyOffsets = globalAdjacencyOffsets[pageIndex] = new long[pageSize];

            long[][] localWeightOffsets = new long[globalWeightOffsets.length][];
            for (int i = 0; i < globalWeightOffsets.length; i++) {
                localWeightOffsets[i] = globalWeightOffsets[i][pageIndex] = new long[pageSize];
            }

            localBuilders[pageIndex] = globalBuilder.threadLocalRelationshipsBuilder(
                    localAdjacencyOffsets,
                    localWeightOffsets
            );
            localBuilders[pageIndex].prepare();
        }

        void finishPreparation() {
            globalBuilder.setGlobalAdjacencyOffsets(AdjacencyOffsets.of(globalAdjacencyOffsets, pageSize));
            AdjacencyOffsets[] globalWeights = new AdjacencyOffsets[globalWeightOffsets.length];
            Arrays.setAll(globalWeights, i -> {
                long[][] globalWeightOffset = globalWeightOffsets[i];
                if (globalWeightOffset != null) {
                    return AdjacencyOffsets.of(globalWeightOffset, pageSize);
                }
                return null;
            });
            globalBuilder.setGlobalPropertyOffsets(globalWeights);
        }

        @Override
        void addAll(
                long[] batch,
                long[] targets,
                long[][] propertyValues,
                int[] offsets,
                int length,
                AllocationTracker tracker) {
            int pageShift = this.pageShift;
            long pageMask = this.pageMask;

            ThreadLocalRelationshipsBuilder builder = null;
            int lastPageIndex = -1;
            int endOffset, startOffset = 0;
            try {
                for (int i = 0; i < length; ++i) {
                    endOffset = offsets[i];

                    // if there are no rels for this node, just go to next
                    if (endOffset <= startOffset) {
                        continue;
                    }

                    long source = batch[startOffset << 2];
                    int pageIndex = (int) (source >>> pageShift);

                    if (pageIndex > lastPageIndex) {
                        // switch to the builder for this page
                        if (builder != null) {
                            builder.unlock();
                        }
                        builder = localBuilders[pageIndex];
                        builder.lock();
                        lastPageIndex = pageIndex;
                    }

                    int localId = (int) (source & pageMask);

                    CompressedLongArray compressedTargets = this.compressedAdjacencyLists[pageIndex][localId];
                    if (compressedTargets == null) {
                        compressedTargets = new CompressedLongArray(tracker, propertyValues == null ? 0 : propertyValues.length);
                        this.compressedAdjacencyLists[pageIndex][localId] = compressedTargets;
                    }

                    if (propertyValues == null) {
                        compressedTargets.add(targets, startOffset, endOffset);
                    } else {
                        compressedTargets.add(targets, propertyValues, startOffset, endOffset);
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
            Runnable[] runnables = new Runnable[localBuilders.length];
            Arrays.setAll(runnables, index -> () -> {
                ThreadLocalRelationshipsBuilder builder = localBuilders[index];
                CompressedLongArray[] allTargets = compressedAdjacencyLists[index];
                LongsRef buffer = buffers[index];
                long importedRelationships = 0L;
                for (int localId = 0; localId < allTargets.length; ++localId) {
                    CompressedLongArray compressedAdjacencyList = allTargets[localId];
                    if (compressedAdjacencyList != null) {
                        importedRelationships += builder.applyVariableDeltaEncoding(
                                compressedAdjacencyList,
                                buffer,
                                localId);

                        allTargets[localId] = null;
                    }
                }
                relationshipCounter.add(importedRelationships);
            });
            return Arrays.asList(runnables);
        }

        @Override
        int[] getPropertyKeyIds() {
            return propertyKeyIds;
        }

        @Override
        double[] getDefaultValues() {
            return defaultValues;
        }

        @Override
        boolean atLeastOnePropertyToLoad() {
            return atLeastOnePropertyToLoad;
        }
    }

    private static final class NoAdjacency extends AdjacencyBuilder {

        private static final AdjacencyBuilder INSTANCE = new NoAdjacency();

        @Override
        void addAll(
                long[] batch,
                long[] targets,
                long[][] propertyValues,
                int[] offsets,
                int length,
                AllocationTracker tracker) {
        }

        @Override
        Collection<Runnable> flushTasks() {
            return Collections.emptyList();
        }

        @Override
        int[] getPropertyKeyIds() {
            return new int[0];
        }

        @Override
        double[] getDefaultValues() {
            return new double[0];
        }

        @Override
        boolean atLeastOnePropertyToLoad() {
            return false;
        }
    }
}
