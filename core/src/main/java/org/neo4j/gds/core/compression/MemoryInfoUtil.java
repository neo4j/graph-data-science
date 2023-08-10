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
package org.neo4j.gds.core.compression;

import org.neo4j.gds.core.compression.common.BlockStatistics;
import org.neo4j.gds.core.compression.common.MemoryTracker;

import java.util.Optional;

public final class MemoryInfoUtil {

    public static ImmutableMemoryInfo.Builder builder(MemoryTracker memoryTracker, Optional<BlockStatistics> blockStatistics) {
        var builder = ImmutableMemoryInfo.builder()
            .heapAllocations(memoryTracker.heapAllocations())
            .nativeAllocations(memoryTracker.nativeAllocations())
            .pageSizes(memoryTracker.pageSizes())
            .headerBits(memoryTracker.headerBits())
            .headerAllocations(memoryTracker.headerAllocations());

        blockStatistics.ifPresent(statistics -> builder
            .blockCount(statistics.blockCount())
            .blockLengths(statistics.blockLengths())
            .stdDevBits(statistics.stdDevBits())
            .meanBits(statistics.meanBits())
            .medianBits(statistics.medianBits())
            .blockLengths(statistics.blockLengths())
            .maxBits(statistics.maxBits())
            .minBits(statistics.minBits())
            .indexOfMinValue(statistics.indexOfMinValue())
            .indexOfMaxValue(statistics.indexOfMaxValue())
            .headTailDiffBits(statistics.headTailDiffBits())
            .bestMaxDiffBits(statistics.bestMaxDiffBits())
            .pforExceptions(statistics.exceptions())
        );

        return builder;
    }

    private MemoryInfoUtil() {}
}
