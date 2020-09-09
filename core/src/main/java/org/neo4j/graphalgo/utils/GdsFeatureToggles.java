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
package org.neo4j.graphalgo.utils;

import org.neo4j.util.FeatureToggles;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public final class GdsFeatureToggles {

    public static final boolean USE_PRE_AGGREGATION_DEFAULT_SETTING = false;
    private static final boolean USE_PRE_AGGREGATION_FLAG = FeatureToggles.flag(
        GdsFeatureToggles.class,
        "usePreAggregation",
        USE_PRE_AGGREGATION_DEFAULT_SETTING
    );
    public static final AtomicBoolean USE_PRE_AGGREGATION = new AtomicBoolean(USE_PRE_AGGREGATION_FLAG);


    public static final boolean SKIP_ORPHANS_DEFAULT_SETTING = false;
    private static final boolean SKIP_ORPHANS_FLAG = FeatureToggles.flag(
        GdsFeatureToggles.class,
        "skipOrphans",
        SKIP_ORPHANS_DEFAULT_SETTING
    );
    public static final AtomicBoolean SKIP_ORPHANS = new AtomicBoolean(SKIP_ORPHANS_FLAG);

    // Prevents full GC more often as not so much consecutive memory is allocated in one go as
    // compared to a page shift of 30 or 32. See https://github.com/neo4j-contrib/neo4j-graph-algorithms/pull/859#discussion_r272262734.
    // Feature toggle is there for testing: org.neo4j.graphalgo.core.huge.loader.HugeGraphLoadingTest#testPropertyLoading
    public static final int MAX_ARRAY_LENGTH_SHIFT_DEFAULT_SETTING = 28;
    private static final int MAX_ARRAY_LENGTH_SHIFT_FLAG = FeatureToggles.getInteger(
        GdsFeatureToggles.class,
        "maxArrayLengthShift",
        MAX_ARRAY_LENGTH_SHIFT_DEFAULT_SETTING
    );
    public static final AtomicInteger MAX_ARRAY_LENGTH_SHIFT = new AtomicInteger(MAX_ARRAY_LENGTH_SHIFT_FLAG);

    public static final boolean USE_KERNEL_TRACKER_DEFAULT_SETTING = false;
    private static final boolean USE_KERNEL_TRACKER_FLAG = FeatureToggles.flag(
        GdsFeatureToggles.class,
        "useKernelTracker",
        USE_KERNEL_TRACKER_DEFAULT_SETTING
    );
    public static final AtomicBoolean USE_KERNEL_TRACKER = new AtomicBoolean(USE_KERNEL_TRACKER_FLAG);

    private GdsFeatureToggles() {}
}
