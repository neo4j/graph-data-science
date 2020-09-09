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
package org.neo4j.graphalgo;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.utils.GdsFeatureToggles;
import org.neo4j.graphdb.QueryExecutionException;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FeatureToggleProcTest extends BaseProcTest {

    @BeforeEach
    void setUp() throws Exception {
        registerProcedures(FeatureToggleProc.class);
    }

    @Test
    void toggleSkipOrphanNodes() {
        var skipOrphanNodes = GdsFeatureToggles.SKIP_ORPHANS.get();
        runQuery("CALL gds.features.importer.skipOrphanNodes($value)", Map.of("value", !skipOrphanNodes));
        assertEquals(!skipOrphanNodes, GdsFeatureToggles.SKIP_ORPHANS.get());
        runQuery("CALL gds.features.importer.skipOrphanNodes($value)", Map.of("value", skipOrphanNodes));
        assertEquals(skipOrphanNodes, GdsFeatureToggles.SKIP_ORPHANS.get());
    }

    @Test
    void resetSkipOrphanNodes() {
        var defaultValue = GdsFeatureToggles.SKIP_ORPHANS_DEFAULT_SETTING;
        GdsFeatureToggles.SKIP_ORPHANS.set(!defaultValue);
        assertCypherResult(
            "CALL gds.features.importer.skipOrphanNodes.reset()",
            List.of(Map.of("enabled", defaultValue))
        );
        assertEquals(defaultValue, GdsFeatureToggles.SKIP_ORPHANS.get());
    }

    @Test
    void toggleusePreAggregation() {
        var usePreAggregation = GdsFeatureToggles.USE_PRE_AGGREGATION.get();
        runQuery("CALL gds.features.importer.usePreAggregation($value)", Map.of("value", !usePreAggregation));
        assertEquals(!usePreAggregation, GdsFeatureToggles.USE_PRE_AGGREGATION.get());
        runQuery("CALL gds.features.importer.usePreAggregation($value)", Map.of("value", usePreAggregation));
        assertEquals(usePreAggregation, GdsFeatureToggles.USE_PRE_AGGREGATION.get());
    }

    @Test
    void resetUsePreAggregation() {
        var defaultValue = GdsFeatureToggles.USE_PRE_AGGREGATION_DEFAULT_SETTING;
        GdsFeatureToggles.USE_PRE_AGGREGATION.set(!defaultValue);
        assertCypherResult(
            "CALL gds.features.importer.usePreAggregation.reset()",
            List.of(Map.of("enabled", defaultValue))
        );
        assertEquals(defaultValue, GdsFeatureToggles.USE_PRE_AGGREGATION.get());
    }

    @Test
    void toggleUseKernelTracker() {
        var useKernelTracker = GdsFeatureToggles.USE_KERNEL_TRACKER.get();
        runQuery("CALL gds.features.useKernelTracker($value)", Map.of("value", !useKernelTracker));
        assertEquals(!useKernelTracker, GdsFeatureToggles.USE_KERNEL_TRACKER.get());
        runQuery("CALL gds.features.useKernelTracker($value)", Map.of("value", useKernelTracker));
        assertEquals(useKernelTracker, GdsFeatureToggles.USE_KERNEL_TRACKER.get());
    }

    @Test
    void resetUseKernelTracker() {
        var defaultValue = GdsFeatureToggles.USE_KERNEL_TRACKER_DEFAULT_SETTING;
        GdsFeatureToggles.USE_KERNEL_TRACKER.set(!defaultValue);
        assertCypherResult(
            "CALL gds.features.useKernelTracker.reset()",
            List.of(Map.of("enabled", defaultValue))
        );
        assertEquals(defaultValue, GdsFeatureToggles.USE_KERNEL_TRACKER.get());
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
