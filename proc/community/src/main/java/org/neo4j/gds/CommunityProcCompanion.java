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

import org.neo4j.gds.api.properties.nodes.LongNodePropertyValues;
import org.neo4j.gds.api.properties.nodes.NodeProperty;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.collections.HugeSparseLongArray;
import org.neo4j.gds.config.CommunitySizeConfig;
import org.neo4j.gds.config.ConcurrencyConfig;
import org.neo4j.gds.config.ConsecutiveIdsConfig;
import org.neo4j.gds.config.SeedConfig;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.nodeproperties.ConsecutiveLongNodePropertyValues;
import org.neo4j.gds.nodeproperties.LongIfChangedNodePropertyValues;
import org.neo4j.gds.result.CommunityStatistics;
import org.neo4j.values.storable.LongValue;
import org.neo4j.values.storable.Value;

import java.util.function.Supplier;

public final class CommunityProcCompanion {

    private CommunityProcCompanion() {}

    public static <CONFIG extends ConcurrencyConfig & SeedConfig & ConsecutiveIdsConfig> NodePropertyValues nodeProperties(
        CONFIG config,
        String resultProperty,
        LongNodePropertyValues propertyValues,
        Supplier<NodeProperty> seedPropertySupplier
    ) {
        var consecutiveIds = config.consecutiveIds();
        var isIncremental = config.isIncremental();
        var resultPropertyEqualsSeedProperty = isIncremental && resultProperty.equals(config.seedProperty());

        if (resultPropertyEqualsSeedProperty && !consecutiveIds) {
            var incrementalNodePropertyValues = LongIfChangedNodePropertyValues.of(seedPropertySupplier.get(), propertyValues);
            return communitySizeFilter(incrementalNodePropertyValues, config);
        } else if (consecutiveIds && !isIncremental) {
            var sizeFilteredPropertyValues = communitySizeFilter(propertyValues, config);
            return new ConsecutiveLongNodePropertyValues(sizeFilteredPropertyValues);
        } else {
            return communitySizeFilter(propertyValues, config);
        }
    }

    private static <CONFIG extends ConcurrencyConfig & SeedConfig & ConsecutiveIdsConfig> LongNodePropertyValues communitySizeFilter(
        LongNodePropertyValues result,
        CONFIG config
    ) {
        if (config instanceof CommunitySizeConfig) {
            var finalResult = result;
            result = ((CommunitySizeConfig) config)
                .minCommunitySize()
                .map(size -> applySizeFilter(finalResult, size, config.concurrency()))
                .orElse(result);
        }

        return result;
    }

    private static LongNodePropertyValues applySizeFilter(
        LongNodePropertyValues nodeProperties,
        long size,
        int concurrency
    ) {
        var communitySizes = CommunityStatistics.communitySizes(
            nodeProperties.maxIndex(),
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

        CommunitySizeFilter(LongNodePropertyValues properties, HugeSparseLongArray communitySizes, long minCommunitySize) {
            this.properties = properties;
            this.communitySizes = communitySizes;
            this.minCommunitySize = minCommunitySize;
        }

        @Override
        public long valuesStored() {
            return properties.valuesStored();
        }

        @Override
        public long maxIndex() {
            return properties.maxIndex();
        }

        @Override
        public long longValue(long nodeId) {
            return properties.longValue(nodeId);
        }

        /**
         * Returning null indicates that the value is not written to Neo4j.
         *
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

            return communitySizes.get(communityId) >= minCommunitySize ? value : null;
        }
    }
}
