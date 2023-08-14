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
package org.neo4j.gds.algorithms.community;

import org.neo4j.gds.api.properties.nodes.LongNodePropertyValues;
import org.neo4j.gds.api.properties.nodes.NodeProperty;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.collections.hsa.HugeSparseLongArray;
import org.neo4j.gds.config.CommunitySizeConfig;
import org.neo4j.gds.config.ConcurrencyConfig;
import org.neo4j.gds.config.ConsecutiveIdsConfig;
import org.neo4j.gds.config.SeedConfig;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.result.CommunityStatistics;
import org.neo4j.values.storable.LongValue;
import org.neo4j.values.storable.Value;

import java.util.Optional;
import java.util.function.Supplier;

final class CommunityResultCompanion {

    private CommunityResultCompanion() {}

    public static <CONFIG extends ConcurrencyConfig & SeedConfig & ConsecutiveIdsConfig> NodePropertyValues nodeProperties(
        CONFIG config,
        String resultProperty,
        LongNodePropertyValues nodeProperties,
        Supplier<NodeProperty> seedPropertySupplier
    ) {
        LongNodePropertyValues result = considerSeedProperty(
            config,
            resultProperty,
            nodeProperties,
            seedPropertySupplier
        );
        return considerConsecutiveIdsFlagAndSize(config, result);
    }

    public static NodePropertyValues nodePropertyValues(
        boolean incremental, boolean consecutiveIds, LongNodePropertyValues nodeProperties, Optional<Long> minCommunitySize, int concurrency
    ) {
        var resultAfterMinFilter = minCommunitySize
            .map(size -> applySizeFilter(nodeProperties, size, concurrency))
            .orElse(nodeProperties);
        return considerConsecutiveIdsFlag(consecutiveIds, incremental, resultAfterMinFilter);
    }

    public static <CONFIG extends ConcurrencyConfig & SeedConfig & ConsecutiveIdsConfig> NodePropertyValues nodeProperties(
        CONFIG config,
        LongNodePropertyValues nodeProperties
    ) {
        return considerConsecutiveIdsFlagAndSize(config, nodeProperties);
    }

    public static <CONFIG extends ConcurrencyConfig> LongNodePropertyValues considerSizeFilter(
        CONFIG config,
        LongNodePropertyValues result
    ) {
        if (config instanceof CommunitySizeConfig) {
            return considerSizeFilter(((CommunitySizeConfig) config).minCommunitySize(), result, config.concurrency());
        }
        return result;
    }

    public static LongNodePropertyValues considerSizeFilter(
        Optional<Long> minCommunitySize,
        LongNodePropertyValues result,
        int concurrency
    ) {
        return minCommunitySize
            .map(size -> applySizeFilter(result, size, concurrency))
            .orElse(result);
    }

    private static <CONFIG extends ConcurrencyConfig & SeedConfig & ConsecutiveIdsConfig> LongNodePropertyValues considerSeedProperty(
        CONFIG config,
        String resultProperty,
        LongNodePropertyValues nodeProperties,
        Supplier<NodeProperty> seedPropertySupplier
    ) {
        var consecutiveIds = config.consecutiveIds();
        var isIncremental = config.isIncremental();
        var resultPropertyEqualsSeedProperty = isIncremental && resultProperty.equals(config.seedProperty());

        if (resultPropertyEqualsSeedProperty && !consecutiveIds) {
            return LongIfChangedNodePropertyValues.of(seedPropertySupplier.get(), nodeProperties);
        }

        return nodeProperties;
    }

    private static <CONFIG extends ConcurrencyConfig & SeedConfig & ConsecutiveIdsConfig> LongNodePropertyValues considerConsecutiveIdsFlag(
        CONFIG config,
        LongNodePropertyValues nodeProperties
    ) {
        var consecutiveIds = config.consecutiveIds();
        var isIncremental = config.isIncremental();
        if (consecutiveIds && !isIncremental) {
            return new ConsecutiveLongNodePropertyValues(nodeProperties);
        }
        
        return nodeProperties;
    }

    private static LongNodePropertyValues considerConsecutiveIdsFlag(
        boolean consecutiveIds, boolean isIncremental, LongNodePropertyValues nodeProperties
    ) {
        if (consecutiveIds && !isIncremental) {
            return new ConsecutiveLongNodePropertyValues(nodeProperties);
        }

        return nodeProperties;
    }

    private static <CONFIG extends ConcurrencyConfig & SeedConfig & ConsecutiveIdsConfig> LongNodePropertyValues considerConsecutiveIdsFlagAndSize(
        CONFIG config,
        LongNodePropertyValues nodeProperties
    ) {
        var resultAfterMinFilter = considerSizeFilter(config, nodeProperties);
        return considerConsecutiveIdsFlag(config, resultAfterMinFilter);
    }

    private static LongNodePropertyValues considerConsecutiveIdsFlagAndSize(
        boolean incremental, boolean consecutiveIds, LongNodePropertyValues nodeProperties, Optional<Long> minCommunitySize, int concurrency
    ) {
        var resultAfterMinFilter = considerSizeFilter(minCommunitySize, nodeProperties, concurrency);
        return considerConsecutiveIdsFlag(consecutiveIds, incremental, resultAfterMinFilter);
    }

    private static LongNodePropertyValues applySizeFilter(
        LongNodePropertyValues nodeProperties,
        long size,
        int concurrency
    ) {
        var communitySizes = CommunityStatistics.communitySizes(
            nodeProperties.nodeCount(),
            nodeProperties::longValue,
            Pools.DEFAULT,
            concurrency
        );
        return new CommunitySizeFilter(nodeProperties, communitySizes, size);
    }

    private static class CommunitySizeFilter implements LongNodePropertyValues {

        private final LongNodePropertyValues properties;

        private final HugeSparseLongArray communitySizes;

        private final long minCommunitySize;

        CommunitySizeFilter(
            LongNodePropertyValues properties,
            HugeSparseLongArray communitySizes,
            long minCommunitySize
        ) {
            this.properties = properties;
            this.communitySizes = communitySizes;
            this.minCommunitySize = minCommunitySize;
        }

        @Override
        public long nodeCount() {
            return properties.nodeCount();
        }

        @Override
        public long longValue(long nodeId) {
            return properties.longValue(nodeId);
        }

        /**
         * Returning null indicates that the value is not written to Neo4j.
         * <p>
         * The filter is applied in the latest stage before writing to Neo4j.
         * Since the wrapped node properties may have additional logic in value(),
         * we need to check if they already filtered the value. Only in the case
         * where the wrapped properties pass on the value, we can apply a filter.
         */
        @Override
        public Value value(long nodeId) {
            var value = properties.value(nodeId);

            if (value == null) {
                return null;
            }

            // This cast is safe since we handle LongNodeProperties.
            var communityId = ((LongValue) value).longValue();

            return isCommunityMinSizeMet(communityId) ? value : null;
        }

        @Override
        public boolean hasValue(long nodeId) {
            return isCommunityMinSizeMet(properties.longValue(nodeId));
        }

        private boolean isCommunityMinSizeMet(long communityId) {
            return communitySizes.get(communityId) >= minCommunitySize;
        }
    }
}
