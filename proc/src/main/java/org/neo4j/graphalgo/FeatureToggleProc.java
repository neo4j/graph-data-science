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

/**
 * General heap of feature toggles we have and procedures to toggle them
 * Please make sure to use the `gds.features.` prefix so that the
 * toggle procedure will be excluded from gds.list
 */
public final class FeatureToggleProc {

    @Procedure("gds.features.importer.skipOrphanNodes")
    @Description("Toggle whether orphan nodes should be skipped during import")
    public void skipOrphanNodes(@Name(value = "skipOrphanNodes") boolean skipOrphanNodes) {
        GdsFeatureToggles.SKIP_ORPHANS.set(skipOrphanNodes);
    }

    @Procedure("gds.features.importer.usePreAggregation")
    @Description("Toggle whether the importer should pre-aggregate relationships")
    public void usePreAggregation(@Name(value = "usePreAggregation") boolean usePreAggregation) {
        GdsFeatureToggles.USE_PRE_AGGREGATION.set(usePreAggregation);
    }

    @Procedure("gds.features.useKernelTracker")
    @Description("Toggle whether the native memory tracking feature on Neo4j 4.1+ should be used")
    public void useKernelTracker(@Name(value = "useKernelTracker") boolean useKernelTracker) {
        GdsFeatureToggles.USE_KERNEL_TRACKER.set(useKernelTracker);
    }
}
