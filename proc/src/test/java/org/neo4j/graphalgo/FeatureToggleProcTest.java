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

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FeatureToggleProcTest extends BaseProcTest {

    @BeforeEach
    void setUp() throws Exception {
        registerProcedures(FeatureToggleProc.class);
    }

    @Test
    void toggleSkippingOrphanNodes() {
        var skipOrphanNodes = GdsFeatureToggles.SKIP_ORPHANS.get();
        runQuery("CALL gds.features.importer.skipOrphanNodes($value)", Map.of("value", !skipOrphanNodes));
        assertEquals(!skipOrphanNodes, GdsFeatureToggles.SKIP_ORPHANS.get());
        runQuery("CALL gds.features.importer.skipOrphanNodes($value)", Map.of("value", skipOrphanNodes));
        assertEquals(skipOrphanNodes, GdsFeatureToggles.SKIP_ORPHANS.get());
    }

    @Test
    void toggleUsingPreAggregation() {
        var usePreAggregation = GdsFeatureToggles.USE_PRE_AGGREGATION.get();
        runQuery("CALL gds.features.importer.usePreAggregation($value)", Map.of("value", !usePreAggregation));
        assertEquals(!usePreAggregation, GdsFeatureToggles.USE_PRE_AGGREGATION.get());
        runQuery("CALL gds.features.importer.usePreAggregation($value)", Map.of("value", usePreAggregation));
        assertEquals(usePreAggregation, GdsFeatureToggles.USE_PRE_AGGREGATION.get());
    }

    @Test
    void toggleUseKernelTracker() {
        var useKernelTracker = GdsFeatureToggles.USE_KERNEL_TRACKER.get();
        runQuery("CALL gds.features.useKernelTracker($value)", Map.of("value", !useKernelTracker));
        assertEquals(!useKernelTracker, GdsFeatureToggles.USE_KERNEL_TRACKER.get());
        runQuery("CALL gds.features.useKernelTracker($value)", Map.of("value", useKernelTracker));
        assertEquals(useKernelTracker, GdsFeatureToggles.USE_KERNEL_TRACKER.get());
    }
}
