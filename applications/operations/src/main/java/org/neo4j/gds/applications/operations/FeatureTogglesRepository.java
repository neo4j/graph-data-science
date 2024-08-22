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
package org.neo4j.gds.applications.operations;

import org.neo4j.gds.utils.GdsFeatureToggles;

/**
 * Let's encapsulate feature toggles and eventually make them not a global singleton.
 */
public class FeatureTogglesRepository {
    boolean resetUseMixedAdjacencyList() {
        GdsFeatureToggles.USE_MIXED_ADJACENCY_LIST.reset();

        return GdsFeatureToggles.USE_MIXED_ADJACENCY_LIST.isEnabled();
    }

    boolean resetUsePackedAdjacencyList() {
        GdsFeatureToggles.USE_PACKED_ADJACENCY_LIST.reset();

        return GdsFeatureToggles.USE_PACKED_ADJACENCY_LIST.isEnabled();
    }

    boolean resetUseUncompressedAdjacencyList() {
        GdsFeatureToggles.USE_UNCOMPRESSED_ADJACENCY_LIST.reset();

        return GdsFeatureToggles.USE_UNCOMPRESSED_ADJACENCY_LIST.isEnabled();
    }

    void setPagesPerThread(int value) {
        GdsFeatureToggles.PAGES_PER_THREAD.set(value);
    }

    void setUseMixedAdjacencyList(boolean value) {
        GdsFeatureToggles.USE_MIXED_ADJACENCY_LIST.toggle(value);
    }

    void setUsePackedAdjacencyList(boolean value) {
        GdsFeatureToggles.USE_PACKED_ADJACENCY_LIST.toggle(value);
    }

    void setUseUncompressedAdjacencyList(boolean value) {
        GdsFeatureToggles.USE_UNCOMPRESSED_ADJACENCY_LIST.toggle(value);
    }
}
