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

import org.neo4j.graphalgo.api.HugeWeightMapping;
import org.neo4j.graphalgo.core.loading.ReadHelper;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.BitUtil;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.Read;

import java.util.Arrays;

class HugeWeightMapBuilder {
    private final AllocationTracker tracker;

    private final int weightProperty;
    private final double defaultWeight;

    private int pageSize;
    private HugeWeightMap.Page[] pages;
    private HugeWeightMap.Page page;

    HugeWeightMapBuilder(AllocationTracker tracker, int weightProperty, double defaultWeight) {
        this.tracker = tracker;
        this.weightProperty = weightProperty;
        this.defaultWeight = defaultWeight;
    }

    private HugeWeightMapBuilder(
            AllocationTracker tracker,
            int weightProperty,
            double defaultWeight,
            HugeWeightMap.Page page) {
        this.tracker = tracker;
        this.weightProperty = weightProperty;
        this.defaultWeight = defaultWeight;
        this.page = page;
    }

    boolean loadsWeights() {
        return true;
    }

    void prepare(int numberOfPages, int pageSize) {
        assert pageSize == 0 || BitUtil.isPowerOfTwo(pageSize);
        this.pageSize = pageSize;
        pages = new HugeWeightMap.Page[numberOfPages];
    }

    HugeWeightMapBuilder threadLocalCopy(int threadIndex, int batchSize) {
        assert pageSize == 0 || batchSize <= pageSize;
        HugeWeightMap.Page page = new HugeWeightMap.Page(batchSize, tracker);
        pages[threadIndex] = page;
        return new HugeWeightMapBuilder(tracker, weightProperty, defaultWeight, page);
    }

    void finish(int numberOfPages) {
        if (numberOfPages < pages.length) {
            pages = Arrays.copyOf(pages, numberOfPages);
        }
    }

    HugeWeightMapping build() {
        return HugeWeightMap.of(pages, pageSize, defaultWeight, tracker);
    }

    int load(
            long relationshipReference,
            long propertiesReference,
            long target,
            int localSource,
            CursorFactory cursors,
            Read read) {
        try (PropertyCursor pc = cursors.allocatePropertyCursor()) {
            read.relationshipProperties(relationshipReference, propertiesReference, pc);
            double weight = ReadHelper.readProperty(pc, weightProperty, defaultWeight);
            if (weight != defaultWeight) {
                addWeight(localSource, target, weight);
                return 1;
            }
            return 0;
        }
    }

    private synchronized void addWeight(int localIndex, long target, double weight) {
        page.put(localIndex, target, weight);
    }

    static class NullBuilder extends HugeWeightMapBuilder {

        private final double defaultValue;

        NullBuilder(double defaultValue) {
            super(AllocationTracker.EMPTY, -1, defaultValue);
            this.defaultValue = defaultValue;
        }

        @Override
        boolean loadsWeights() {
            return false;
        }

        @Override
        void prepare(int numberOfPages, int pageSize) {
        }

        @Override
        HugeWeightMapBuilder threadLocalCopy(int threadIndex, int batchSize) {
            return this;
        }

        @Override
        void finish(int numberOfPages) {
        }

        @Override
        HugeWeightMapping build() {
            return new HugeNullWeightMap(defaultValue);
        }

        @Override
        int load(
                long relationshipReference,
                long propertiesReference,
                long target,
                int localSource,
                CursorFactory cursors,
                Read read) {
            return 0;
        }
    }
}
