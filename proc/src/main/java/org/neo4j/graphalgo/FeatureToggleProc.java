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

import org.neo4j.graphalgo.utils.GdsFeatureToggles;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.stream.Stream;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

/**
 * General heap of feature toggles we have and procedures to toggle them
 * Please make sure to use the `gds.features.` prefix so that the
 * toggle procedure will be excluded from gds.list
 */
public final class FeatureToggleProc {

    @Procedure("gds.features.importer.skipOrphanNodes")
    @Description("Toggle whether orphan nodes should be skipped during import")
    public void skipOrphanNodes(@Name(value = "skipOrphanNodes") boolean skipOrphanNodes) {
        GdsFeatureToggles.SKIP_ORPHANS.toggle(skipOrphanNodes);
    }

    @Procedure("gds.features.importer.skipOrphanNodes.reset")
    @Description("Set the behavior of whether to skip orphan nodes to the default. That value is returned.")
    public Stream<FeatureState> resetSkipOrphanNodes() {
        GdsFeatureToggles.SKIP_ORPHANS.reset();
        return Stream.of(new FeatureState(GdsFeatureToggles.SKIP_ORPHANS.isToggled()));
    }

    @Procedure("gds.features.importer.usePreAggregation")
    @Description("Toggle whether the importer should pre-aggregate relationships")
    public void usePreAggregation(@Name(value = "usePreAggregation") boolean usePreAggregation) {
        GdsFeatureToggles.USE_PRE_AGGREGATION.toggle(usePreAggregation);
    }

    @Procedure("gds.features.importer.usePreAggregation.reset")
    @Description("Set the behavior of whether to pre-aggregate relationships to the default. That value is returned.")
    public Stream<FeatureState> resetUsePreAggregation() {
        GdsFeatureToggles.USE_PRE_AGGREGATION.reset();
        return Stream.of(new FeatureState(GdsFeatureToggles.USE_PRE_AGGREGATION.isToggled()));
    }

    @Procedure("gds.features.useKernelTracker")
    @Description("Toggle whether the native memory tracking feature on Neo4j 4.1+ should be used")
    public void useKernelTracker(@Name(value = "useKernelTracker") boolean useKernelTracker) {
        GdsFeatureToggles.USE_KERNEL_TRACKER.toggle(useKernelTracker);
    }

    @Procedure("gds.features.useKernelTracker.reset")
    @Description("Set the behavior of whether to use the native memory tracking to the default. That value is returned.")
    public Stream<FeatureState> resetUseKernelTracker() {
        GdsFeatureToggles.USE_KERNEL_TRACKER.reset();
        return Stream.of(new FeatureState(GdsFeatureToggles.USE_KERNEL_TRACKER.isToggled()));
    }

    @Procedure("gds.features.maxArrayLengthShift")
    @Description("Toggle how large arrays are allowed to get before they are being paged; value is a power of two")
    public void maxArrayLengthShift(@Name(value = "maxArrayLengthShift") long maxArrayLengthShift) {
        if (maxArrayLengthShift <= 0 || maxArrayLengthShift >= Integer.SIZE) {
            throw new IllegalArgumentException(formatWithLocale(
                "Invalid value for maxArrayLengthShift, must be in (0, %d)",
                Integer.SIZE
            ));
        }
        GdsFeatureToggles.MAX_ARRAY_LENGTH_SHIFT.set((int) maxArrayLengthShift);
    }

    @Procedure("gds.features.maxArrayLengthShift.reset")
    @Description("Set the value of the max array size before paging to the default. That value is returned.")
    public Stream<FeatureValue> resetMaxArrayLengthShift() {
        GdsFeatureToggles.MAX_ARRAY_LENGTH_SHIFT.set(GdsFeatureToggles.MAX_ARRAY_LENGTH_SHIFT_DEFAULT_SETTING);
        return Stream.of(new FeatureValue(GdsFeatureToggles.MAX_ARRAY_LENGTH_SHIFT_DEFAULT_SETTING));
    }

    public static final class FeatureState {
        public final boolean enabled;

        FeatureState(boolean enabled) {
            this.enabled = enabled;
        }
    }

    public static final class FeatureValue {
        public final long value;

        FeatureValue(long value) {
            this.value = value;
        }
    }
}
