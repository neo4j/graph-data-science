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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.junit.annotation.Edition;
import org.neo4j.gds.junit.annotation.GdsEditionTest;
import org.neo4j.graphalgo.utils.GdsFeatureToggles;
import org.neo4j.graphdb.QueryExecutionException;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;
import static org.neo4j.graphalgo.utils.GdsFeatureToggles.SKIP_ORPHANS;
import static org.neo4j.graphalgo.utils.GdsFeatureToggles.USE_BIT_ID_MAP;
import static org.neo4j.graphalgo.utils.GdsFeatureToggles.USE_KERNEL_TRACKER;
import static org.neo4j.graphalgo.utils.GdsFeatureToggles.USE_PARALLEL_PROPERTY_VALUE_INDEX;
import static org.neo4j.graphalgo.utils.GdsFeatureToggles.USE_PRE_AGGREGATION;
import static org.neo4j.graphalgo.utils.GdsFeatureToggles.USE_PROPERTY_VALUE_INDEX;
import static org.neo4j.graphalgo.utils.GdsFeatureToggles.USE_REORDERED_ADJACENCY_LIST;
import static org.neo4j.graphalgo.utils.GdsFeatureToggles.USE_UNCOMPRESSED_ADJACENCY_LIST;

class FeatureToggleProcTest extends BaseProcTest {

    @BeforeEach
    void setUp() throws Exception {
        registerProcedures(FeatureToggleProc.class);
    }

    @Test
    void toggleSkipOrphanNodes() {
        var skipOrphanNodes = SKIP_ORPHANS.isEnabled();
        runQuery("CALL gds.features.importer.skipOrphanNodes($value)", Map.of("value", !skipOrphanNodes));
        assertEquals(!skipOrphanNodes, SKIP_ORPHANS.isEnabled());
        runQuery("CALL gds.features.importer.skipOrphanNodes($value)", Map.of("value", skipOrphanNodes));
        assertEquals(skipOrphanNodes, SKIP_ORPHANS.isEnabled());
    }

    @Test
    void resetSkipOrphanNodes() {
        SKIP_ORPHANS.reset();
        assertCypherResult(
            "CALL gds.features.importer.skipOrphanNodes.reset()",
            List.of(Map.of("enabled", false))
        );
        assertEquals(false, SKIP_ORPHANS.isEnabled());
    }

    @Test
    void toggleusePreAggregation() {
        var usePreAggregation = USE_PRE_AGGREGATION.isEnabled();
        runQuery("CALL gds.features.importer.usePreAggregation($value)", Map.of("value", !usePreAggregation));
        assertEquals(!usePreAggregation, USE_PRE_AGGREGATION.isEnabled());
        runQuery("CALL gds.features.importer.usePreAggregation($value)", Map.of("value", usePreAggregation));
        assertEquals(usePreAggregation, USE_PRE_AGGREGATION.isEnabled());
    }

    @Test
    void resetUsePreAggregation() {
        USE_PRE_AGGREGATION.reset();
        assertCypherResult(
            "CALL gds.features.importer.usePreAggregation.reset()",
            List.of(Map.of("enabled", false))
        );
        assertEquals(false, USE_PRE_AGGREGATION.isEnabled());
    }

    @Test
    void toggleUseKernelTracker() {
        var useKernelTracker = USE_KERNEL_TRACKER.isEnabled();
        runQuery("CALL gds.features.useKernelTracker($value)", Map.of("value", !useKernelTracker));
        assertEquals(!useKernelTracker, USE_KERNEL_TRACKER.isEnabled());
        runQuery("CALL gds.features.useKernelTracker($value)", Map.of("value", useKernelTracker));
        assertEquals(useKernelTracker, USE_KERNEL_TRACKER.isEnabled());
    }

    @Test
    void resetUseKernelTracker() {
        USE_KERNEL_TRACKER.reset();
        assertCypherResult(
            "CALL gds.features.useKernelTracker.reset()",
            List.of(Map.of("enabled", false))
        );
        assertEquals(false, USE_KERNEL_TRACKER.isEnabled());
    }

    @Test
    void toggleUsePropertyValueIndex() {
        var usePropertyValueIndex = USE_PROPERTY_VALUE_INDEX.isEnabled();
        runQuery("CALL gds.features.usePropertyValueIndex($value)", Map.of("value", !usePropertyValueIndex));
        assertEquals(!usePropertyValueIndex, USE_PROPERTY_VALUE_INDEX.isEnabled());
        runQuery("CALL gds.features.usePropertyValueIndex($value)", Map.of("value", usePropertyValueIndex));
        assertEquals(usePropertyValueIndex, USE_PROPERTY_VALUE_INDEX.isEnabled());
    }

    @Test
    void resetUsePropertyValueIndex() {
        USE_PROPERTY_VALUE_INDEX.reset();
        assertCypherResult(
            "CALL gds.features.usePropertyValueIndex.reset()",
            List.of(Map.of("enabled", false))
        );
        assertEquals(false, USE_PROPERTY_VALUE_INDEX.isEnabled());
    }

    @Test
    void toggleUseParallelPropertyValueIndex() {
        var useParallelPropertyValueIndex = USE_PARALLEL_PROPERTY_VALUE_INDEX.isEnabled();
        runQuery("CALL gds.features.useParallelPropertyValueIndex($value)", Map.of("value", !useParallelPropertyValueIndex));
        assertEquals(!useParallelPropertyValueIndex, USE_PARALLEL_PROPERTY_VALUE_INDEX.isEnabled());
        runQuery("CALL gds.features.useParallelPropertyValueIndex($value)", Map.of("value", useParallelPropertyValueIndex));
        assertEquals(useParallelPropertyValueIndex, USE_PARALLEL_PROPERTY_VALUE_INDEX.isEnabled());
    }

    @Test
    void resetUseParallelPropertyValueIndex() {
        USE_PARALLEL_PROPERTY_VALUE_INDEX.reset();
        assertCypherResult(
            "CALL gds.features.useParallelPropertyValueIndex.reset()",
            List.of(Map.of("enabled", false))
        );
        assertEquals(false, USE_PARALLEL_PROPERTY_VALUE_INDEX.isEnabled());
    }

    @Test
    void toggleUseUncompressedAdjacencyList() {
        var useUncompressedAdjacencyList = USE_UNCOMPRESSED_ADJACENCY_LIST.isEnabled();
        runQuery("CALL gds.features.useUncompressedAdjacencyList($value)", Map.of("value", !useUncompressedAdjacencyList));
        assertEquals(!useUncompressedAdjacencyList, USE_UNCOMPRESSED_ADJACENCY_LIST.isEnabled());
        runQuery("CALL gds.features.useUncompressedAdjacencyList($value)", Map.of("value", useUncompressedAdjacencyList));
        assertEquals(useUncompressedAdjacencyList, USE_UNCOMPRESSED_ADJACENCY_LIST.isEnabled());
    }

    @Test
    void resetUseUncompressedAdjacencyList() {
        USE_UNCOMPRESSED_ADJACENCY_LIST.reset();
        assertCypherResult(
            "CALL gds.features.useUncompressedAdjacencyList.reset()",
            List.of(Map.of("enabled", false))
        );
        assertEquals(false, USE_UNCOMPRESSED_ADJACENCY_LIST.isEnabled());
    }

    @Test
    void toggleUseReorderedAdjacencyList() {
        var useReorderedAdjacencyList = USE_REORDERED_ADJACENCY_LIST.isEnabled();
        runQuery("CALL gds.features.useReorderedAdjacencyList($value)", Map.of("value", !useReorderedAdjacencyList));
        assertEquals(!useReorderedAdjacencyList, USE_REORDERED_ADJACENCY_LIST.isEnabled());
        runQuery("CALL gds.features.useReorderedAdjacencyList($value)", Map.of("value", useReorderedAdjacencyList));
        assertEquals(useReorderedAdjacencyList, USE_REORDERED_ADJACENCY_LIST.isEnabled());
    }

    @Test
    void resetUseReorderedAdjacencyList() {
        USE_REORDERED_ADJACENCY_LIST.reset();
        assertCypherResult(
            "CALL gds.features.useReorderedAdjacencyList.reset()",
            List.of(Map.of("enabled", false))
        );
        assertEquals(false, USE_REORDERED_ADJACENCY_LIST.isEnabled());
    }

    @GdsEditionTest(Edition.EE)
    void toggleUseBitIdMap() {
        var useBitIdMap = USE_BIT_ID_MAP.isEnabled();
        runQuery("CALL gds.features.useBitIdMap($value)", Map.of("value", !useBitIdMap));
        assertEquals(!useBitIdMap, USE_BIT_ID_MAP.isEnabled());
        runQuery("CALL gds.features.useBitIdMap($value)", Map.of("value", useBitIdMap));
        assertEquals(useBitIdMap, USE_BIT_ID_MAP.isEnabled());
    }

    @GdsEditionTest(Edition.EE)
    void resetUseBitIdMap() {
        USE_BIT_ID_MAP.reset();
        assertCypherResult(
            "CALL gds.features.useBitIdMap.reset()",
            List.of(Map.of("enabled", true))
        );
        assertEquals(true, USE_BIT_ID_MAP.isEnabled());
    }

    @Test
    void toggleUseBitIdMapFailsOnCommunity() {
        assertThatThrownBy(() -> runQuery("CALL gds.features.useBitIdMap(false)"))
            .hasMessageContaining(
                formatWithLocale("Enterprise Edition of the Neo4j Graph Data Science Library")
            );
    }

    @Test
    void resetUseBitIdMapFailsOnCommunity() {
        assertThatThrownBy(() -> runQuery("CALL gds.features.useBitIdMap.reset()"))
            .hasMessageContaining(
                formatWithLocale("Enterprise Edition of the Neo4j Graph Data Science Library")
            );
    }

    @Test
    void toggleMaxArrayLengthShift() {
        var maxArrayLengthShift = GdsFeatureToggles.MAX_ARRAY_LENGTH_SHIFT.get();
        runQuery("CALL gds.features.maxArrayLengthShift($value)", Map.of("value", maxArrayLengthShift + 1));
        assertEquals(maxArrayLengthShift + 1, GdsFeatureToggles.MAX_ARRAY_LENGTH_SHIFT.get());
        runQuery("CALL gds.features.maxArrayLengthShift($value)", Map.of("value", maxArrayLengthShift));
        assertEquals(maxArrayLengthShift, GdsFeatureToggles.MAX_ARRAY_LENGTH_SHIFT.get());
    }

    @Test
    void toggleMaxArrayLengthShiftValidation() {
        var maxArrayLengthShift = GdsFeatureToggles.MAX_ARRAY_LENGTH_SHIFT.get();
        var exception = assertThrows(
            QueryExecutionException.class,
            () -> runQuery("CALL gds.features.maxArrayLengthShift($value)", Map.of("value", 42))
        );
        assertThat(exception)
            .hasRootCauseInstanceOf(IllegalArgumentException.class)
            .hasRootCauseMessage("Invalid value for maxArrayLengthShift, must be in (0, 32)");
        assertEquals(maxArrayLengthShift, GdsFeatureToggles.MAX_ARRAY_LENGTH_SHIFT.get());
    }

    @Test
    void resetMaxArrayLengthShift() {
        var defaultValue = GdsFeatureToggles.MAX_ARRAY_LENGTH_SHIFT_DEFAULT_SETTING;
        GdsFeatureToggles.MAX_ARRAY_LENGTH_SHIFT.set(defaultValue + 1);
        assertCypherResult(
            "CALL gds.features.maxArrayLengthShift.reset()",
            List.of(Map.of("value", (long) defaultValue))
        );
        assertEquals(defaultValue, GdsFeatureToggles.MAX_ARRAY_LENGTH_SHIFT.get());
    }
}
