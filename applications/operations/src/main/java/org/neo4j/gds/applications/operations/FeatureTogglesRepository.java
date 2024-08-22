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

import java.util.Arrays;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;
import static org.neo4j.gds.utils.StringFormatting.toUpperCaseWithLocale;

/**
 * Let's encapsulate feature toggles and eventually make them not a global singleton.
 */
public class FeatureTogglesRepository {
    void enableAdjacencyCompressionMemoryTracking(boolean value) {
        GdsFeatureToggles.ENABLE_ADJACENCY_COMPRESSION_MEMORY_TRACKING.toggle(value);
    }

    void enableArrowDatabaseImport(boolean value) {
        GdsFeatureToggles.ENABLE_ARROW_DATABASE_IMPORT.toggle(value);
    }

    String resetAdjacencyPackingStrategy() {
        ensureAdjacencyPackingEnabled("reset");

        GdsFeatureToggles.ADJACENCY_PACKING_STRATEGY.set(GdsFeatureToggles.ADJACENCY_PACKING_STRATEGY_DEFAULT_SETTING);

        return GdsFeatureToggles.ADJACENCY_PACKING_STRATEGY.get().name();
    }

    boolean resetEnableArrowDatabaseImport() {
        GdsFeatureToggles.ENABLE_ARROW_DATABASE_IMPORT.reset();

        return GdsFeatureToggles.ENABLE_ARROW_DATABASE_IMPORT.isEnabled();
    }

    int resetPagesPerThread() {
        GdsFeatureToggles.PAGES_PER_THREAD.set(GdsFeatureToggles.PAGES_PER_THREAD_DEFAULT_SETTING);

        return GdsFeatureToggles.PAGES_PER_THREAD_DEFAULT_SETTING;
    }

    boolean resetUseMixedAdjacencyList() {
        GdsFeatureToggles.USE_MIXED_ADJACENCY_LIST.reset();

        return GdsFeatureToggles.USE_MIXED_ADJACENCY_LIST.isEnabled();
    }

    boolean resetUsePackedAdjacencyList() {
        GdsFeatureToggles.USE_PACKED_ADJACENCY_LIST.reset();

        return GdsFeatureToggles.USE_PACKED_ADJACENCY_LIST.isEnabled();
    }

    boolean resetUseReorderedAdjacencyList() {
        GdsFeatureToggles.USE_REORDERED_ADJACENCY_LIST.reset();

        return GdsFeatureToggles.USE_REORDERED_ADJACENCY_LIST.isEnabled();
    }

    boolean resetUseUncompressedAdjacencyList() {
        GdsFeatureToggles.USE_UNCOMPRESSED_ADJACENCY_LIST.reset();

        return GdsFeatureToggles.USE_UNCOMPRESSED_ADJACENCY_LIST.isEnabled();
    }

    void setAdjacencyPackingStrategy(String strategyIdentifierAsString) {
        ensureAdjacencyPackingEnabled("set");

        var strategyIdentifier = parseStrategyIdentifier(strategyIdentifierAsString);

        GdsFeatureToggles.ADJACENCY_PACKING_STRATEGY.set(strategyIdentifier);
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

    void setUseReorderedAdjacencyList(boolean value) {
        GdsFeatureToggles.USE_REORDERED_ADJACENCY_LIST.toggle(value);
    }

    void setUseUncompressedAdjacencyList(boolean value) {
        GdsFeatureToggles.USE_UNCOMPRESSED_ADJACENCY_LIST.toggle(value);
    }

    private void ensureAdjacencyPackingEnabled(String operation) {
        if (GdsFeatureToggles.USE_PACKED_ADJACENCY_LIST.isEnabled()) return;

        throw new IllegalStateException("Cannot " + operation + " adjacency packing strategy when packed adjacency list is disabled.");
    }

    private GdsFeatureToggles.AdjacencyPackingStrategy parseStrategyIdentifier(String strategyIdentifierAsString) {
        var canonicalizedStrategyIdentifier = toUpperCaseWithLocale(strategyIdentifierAsString);

        try {
            return GdsFeatureToggles.AdjacencyPackingStrategy.valueOf(canonicalizedStrategyIdentifier);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                formatWithLocale(
                    "Invalid adjacency packing strategy: %s, must be one of %s",
                    strategyIdentifierAsString,
                    Arrays.toString(GdsFeatureToggles.AdjacencyPackingStrategy.values())
                )
            );
        }
    }
}
