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
package org.neo4j.gds;

import org.neo4j.gds.utils.GdsFeatureToggles;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Internal;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.stream.Stream;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

/**
 * General heap of feature toggles we have and procedures to toggle them
 * Please make sure to use the `gds.features.` prefix so that the
 * toggle procedure will be excluded from gds.list
 */
public final class FeatureToggleProc {

    @Internal
    @Procedure("gds.features.importer.skipOrphanNodes")
    @Description("Toggle whether orphan nodes should be skipped during import.")
    public void skipOrphanNodes(@Name(value = "skipOrphanNodes") boolean skipOrphanNodes) {
        GdsFeatureToggles.SKIP_ORPHANS.toggle(skipOrphanNodes);
    }

    @Internal
    @Procedure("gds.features.importer.skipOrphanNodes.reset")
    @Description("Set the behavior of whether to skip orphan nodes to the default. That value is returned.")
    public Stream<FeatureState> resetSkipOrphanNodes() {
        GdsFeatureToggles.SKIP_ORPHANS.reset();
        return Stream.of(new FeatureState(GdsFeatureToggles.SKIP_ORPHANS.isEnabled()));
    }

    @Internal
    @Procedure("gds.features.usePropertyValueIndex")
    @Description("Toggle whether the property value index should be used during node property loading.")
    public void usePropertyValueIndex(@Name(value = "usePropertyValueIndex") boolean usePropertyValueIndex) {
        GdsFeatureToggles.USE_PROPERTY_VALUE_INDEX.toggle(usePropertyValueIndex);
    }

    @Internal
    @Procedure("gds.features.usePropertyValueIndex.reset")
    @Description("Set the behavior of whether to use the property value index to the default. That value is returned.")
    public Stream<FeatureState> resetUsePropertyValueIndex() {
        GdsFeatureToggles.USE_PROPERTY_VALUE_INDEX.reset();
        return Stream.of(new FeatureState(GdsFeatureToggles.USE_PROPERTY_VALUE_INDEX.isEnabled()));
    }

    @Internal
    @Procedure("gds.features.useParallelPropertyValueIndex")
    @Description("Toggle whether the property value index should be read in parallel during node property loading. Only works if usePropertyValueIndex is set as well")
    public void useParallelPropertyValueIndex(@Name(value = "useParallelPropertyValueIndex") boolean useParallelPropertyValueIndex) {
        GdsFeatureToggles.USE_PARALLEL_PROPERTY_VALUE_INDEX.toggle(useParallelPropertyValueIndex);
    }

    @Internal
    @Procedure("gds.features.useParallelPropertyValueIndex.reset")
    @Description("Set the behavior of whether to use the property value index to the default. That value is returned.")
    public Stream<FeatureState> resetUseParallelPropertyValueIndex() {
        GdsFeatureToggles.USE_PARALLEL_PROPERTY_VALUE_INDEX.reset();
        return Stream.of(new FeatureState(GdsFeatureToggles.USE_PARALLEL_PROPERTY_VALUE_INDEX.isEnabled()));
    }

    @Internal
    @Procedure("gds.features.usePartitionedScan")
    @Description("Toggle whether the new partitioned scan API should be used.")
    public void usePartitionedScan(@Name(value = "usePartitionedScan") boolean usePartitionedScan) {
        GdsFeatureToggles.USE_PARTITIONED_SCAN.toggle(usePartitionedScan);
    }

    @Internal
    @Procedure("gds.features.usePartitionedScan.reset")
    @Description("Set the behavior of whether to use the new partitioned scan API to the default. That value is returned.")
    public Stream<FeatureState> resetUsePartitionedScan() {
        GdsFeatureToggles.USE_PARTITIONED_SCAN.reset();
        return Stream.of(new FeatureState(GdsFeatureToggles.USE_PARTITIONED_SCAN.isEnabled()));
    }

    @Internal
    @Procedure("gds.features.maxArrayLengthShift")
    @Description("Toggle how large arrays are allowed to get before they are being paged; value is a power of two.")
    public void maxArrayLengthShift(@Name(value = "maxArrayLengthShift") long maxArrayLengthShift) {
        if (maxArrayLengthShift <= 0 || maxArrayLengthShift >= Integer.SIZE) {
            throw new IllegalArgumentException(formatWithLocale(
                "Invalid value for maxArrayLengthShift, must be in (0, %d)",
                Integer.SIZE
            ));
        }
        GdsFeatureToggles.MAX_ARRAY_LENGTH_SHIFT.set((int) maxArrayLengthShift);
    }

    @Internal
    @Procedure("gds.features.pagesPerThread")
    @Description("Toggle how many pages per thread are being used by the loader.")
    public void pagesPerThread(@Name(value = "pagesPerThread") long pagesPerThread) {
        if (pagesPerThread <= 0 || pagesPerThread > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(formatWithLocale(
                "Invalid value for pagesPerThread: %d, must be a non-zero, positive integer",
                pagesPerThread
            ));
        }
        GdsFeatureToggles.PAGES_PER_THREAD.set((int) pagesPerThread);
    }

    @Internal
    @Procedure("gds.features.useUncompressedAdjacencyList")
    @Description("Toggle whether the adjacency list should be stored uncompressed during graph creation.")
    public void useUncompressedAdjacencyList(@Name(value = "useUncompressedAdjacencyList") boolean useUncompressedAdjacencyList) {
        GdsFeatureToggles.USE_UNCOMPRESSED_ADJACENCY_LIST.toggle(useUncompressedAdjacencyList);
    }

    @Internal
    @Procedure("gds.features.useUncompressedAdjacencyList.reset")
    @Description("Set the default behaviour of whether to store uncompressed adjacency lists during graph creation. That value is returned.")
    public Stream<FeatureState> resetUseUncompressedAdjacencyList() {
        GdsFeatureToggles.USE_UNCOMPRESSED_ADJACENCY_LIST.reset();
        return Stream.of(new FeatureState(GdsFeatureToggles.USE_UNCOMPRESSED_ADJACENCY_LIST.isEnabled()));
    }

    @Internal
    @Procedure("gds.features.useReorderedAdjacencyList")
    @Description("Toggle whether the adjacency list should be reordered during graph creation.")
    public void useReorderedAdjacencyList(@Name(value = "useReorderedAdjacencyList") boolean useReorderedAdjacencyList) {
        GdsFeatureToggles.USE_REORDERED_ADJACENCY_LIST.toggle(useReorderedAdjacencyList);
    }

    @Internal
    @Procedure("gds.features.useReorderedAdjacencyList.reset")
    @Description("Set the default behaviour of whether to reorder adjacency lists during graph creation. That value is returned.")
    public Stream<FeatureState> resetUseReorderedAdjacencyList() {
        GdsFeatureToggles.USE_REORDERED_ADJACENCY_LIST.reset();
        return Stream.of(new FeatureState(GdsFeatureToggles.USE_REORDERED_ADJACENCY_LIST.isEnabled()));
    }

    @Internal
    @Procedure("gds.features.enableArrowDatabaseImport")
    @Description("Enables support for importing Neo4j databases via the GDS Arrow Flight Server.")
    public void enableArrowDatabaseImport(@Name(value = "useReorderedAdjacencyList") boolean useReorderedAdjacencyList) {
        GdsFeatureToggles.ENABLE_ARROW_DATABASE_IMPORT.toggle(useReorderedAdjacencyList);
    }

    @Internal
    @Procedure("gds.features.enableArrowDatabaseImport.reset")
    @Description("Sets the default behaviour for enabling Neo4j database import via the GDS Arrow Flight Server. That value is returned.")
    public Stream<FeatureState> resetEnableArrowDatabaseImport() {
        GdsFeatureToggles.ENABLE_ARROW_DATABASE_IMPORT.reset();
        return Stream.of(new FeatureState(GdsFeatureToggles.ENABLE_ARROW_DATABASE_IMPORT.isEnabled()));
    }

    @Internal
    @Procedure("gds.features.maxArrayLengthShift.reset")
    @Description("Set the value of the max array size before paging to the default. That value is returned.")
    public Stream<FeatureValue> resetMaxArrayLengthShift() {
        GdsFeatureToggles.MAX_ARRAY_LENGTH_SHIFT.set(GdsFeatureToggles.MAX_ARRAY_LENGTH_SHIFT_DEFAULT_SETTING);
        return Stream.of(new FeatureValue(GdsFeatureToggles.MAX_ARRAY_LENGTH_SHIFT_DEFAULT_SETTING));
    }

    @Internal
    @Procedure("gds.features.pagesPerThread.reset")
    @Description("Set the value of pages per thread to the default. That value is returned.")
    public Stream<FeatureValue> resetPagesPerThread() {
        GdsFeatureToggles.PAGES_PER_THREAD.set(GdsFeatureToggles.PAGES_PER_THREAD_DEFAULT_SETTING);
        return Stream.of(new FeatureValue(GdsFeatureToggles.PAGES_PER_THREAD_DEFAULT_SETTING));
    }

    @SuppressWarnings("unused")
    public static final class FeatureState {
        public final boolean enabled;

        public FeatureState(boolean enabled) {
            this.enabled = enabled;
        }
    }

    @SuppressWarnings("unused")
    public static final class FeatureValue {
        public final long value;

        FeatureValue(long value) {
            this.value = value;
        }
    }
}
