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

import org.eclipse.collections.api.block.function.primitive.LongToObjectFunction;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.api.properties.nodes.FilteredNodePropertyValuesMarker;
import org.neo4j.gds.api.properties.nodes.LongArrayNodePropertyValues;
import org.neo4j.gds.api.properties.nodes.LongNodePropertyValues;
import org.neo4j.gds.api.properties.nodes.NodeProperty;
import org.neo4j.gds.api.properties.nodes.NodePropertyContainer;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.collections.hsa.HugeSparseLongArray;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.result.CommunityStatistics;

import java.util.Optional;
import java.util.function.Supplier;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class CommunityCompanion {

    private CommunityCompanion() {}

    public static NodePropertyValues nodePropertyValues(
        boolean consecutiveIds,
        LongNodePropertyValues nodeProperties
    ) {

        if (consecutiveIds) {
            return new ConsecutiveLongNodePropertyValues(nodeProperties);
        }

        return nodeProperties;
    }

    public static NodePropertyValues nodePropertyValues(
        boolean isIncremental,
        String resultProperty,
        String seedProperty,
        boolean consecutiveIds,
        LongNodePropertyValues nodeProperties,
        Supplier<NodeProperty> seedPropertySupplier
    ) {

        if (consecutiveIds) {
            return new ConsecutiveLongNodePropertyValues(nodeProperties);
        }
        if (isIncremental && resultProperty.equals(seedProperty)) {
            nodeProperties = LongIfChangedNodePropertyValues.of(seedPropertySupplier.get(), nodeProperties);
        }


        return nodeProperties;
    }

    public static NodePropertyValues nodePropertyValues(
        boolean incremental,
        String resultProperty,
        String seedProperty,
        boolean consecutiveIds,
        LongNodePropertyValues nodeProperties,
        Optional<Long> minCommunitySize,
        Concurrency concurrency,
        Supplier<NodeProperty> seedPropertySupplier
    ) {
        var resultAfterMinFilter = minCommunitySize
            .map(size -> applySizeFilter(nodeProperties, size, concurrency))
            .orElse(nodeProperties);

        return nodePropertyValues(
            incremental,
            resultProperty,
            seedProperty,
            consecutiveIds,
            resultAfterMinFilter,
            seedPropertySupplier
        );
    }

    public static NodePropertyValues nodePropertyValues(
        boolean consecutiveIds,
        LongNodePropertyValues nodeProperties,
        Optional<Long> minCommunitySize,
        Concurrency concurrency
    ) {
        var resultAfterMinFilter = minCommunitySize
            .map(size -> applySizeFilter(nodeProperties, size, concurrency))
            .orElse(nodeProperties);

        return nodePropertyValues(consecutiveIds, resultAfterMinFilter);
    }

    public static LongArrayNodePropertyValues createIntermediateCommunitiesNodePropertyValues(
        LongToObjectFunction<long[]> intermediateCommunitiesProvider,
        long size
    ) {
        return new LongArrayNodePropertyValues() {
            @Override
            public long nodeCount() {
                return size;
            }

            @Override
            public long[] longArrayValue(long nodeId) {
                return intermediateCommunitiesProvider.apply(nodeId);
            }
        };
    }


    private static LongNodePropertyValues applySizeFilter(
        LongNodePropertyValues nodeProperties,
        long size,
        Concurrency concurrency
    ) {
        var communitySizes = CommunityStatistics.communitySizes(
            nodeProperties.nodeCount(),
            nodeProperties::longValue,
            DefaultPool.INSTANCE,
            concurrency
        );
        return new CommunitySizeFilter(nodeProperties, communitySizes, size);
    }

    private static class CommunitySizeFilter implements LongNodePropertyValues, FilteredNodePropertyValuesMarker {

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

        /**
         * Returning Long.MIN_VALUE indicates that the value should not be written to Neo4j.
         * <p>
         * The filter is applied in the latest stage before writing to Neo4j.
         * Since the wrapped node properties may have additional logic in longValue(),
         * we need to check if they already filtered the value. Only in the case
         * where the wrapped properties pass on the value, we can apply a filter.
         */
        @Override
        public long longValue(long nodeId) {
            var longValue = properties.longValue(nodeId);
            // did the wrapped properties filter out the value?
            if (longValue == Long.MIN_VALUE) {
                return Long.MIN_VALUE;
            }
            // apply our own filter
            return isCommunityMinSizeMet(longValue) ? longValue : Long.MIN_VALUE;
        }

        @Override
        public boolean hasValue(long nodeId) {
            return isCommunityMinSizeMet(properties.longValue(nodeId));
        }

        private boolean isCommunityMinSizeMet(long communityId) {
            return communitySizes.get(communityId) >= minCommunitySize;
        }
    }

    public static NodePropertyValues extractSeedingNodePropertyValues(NodePropertyContainer nodePropertyContainer, String seedingProperty) {
        var nodePropertyValues = nodePropertyContainer.nodeProperties(seedingProperty);

        if (nodePropertyValues == null) return null;

        if (nodePropertyValues.valueType() != ValueType.LONG) {
            throw new IllegalArgumentException(
                formatWithLocale(
                    "Provided seeding property `%s` does not comprise exclusively of long values",
                    seedingProperty
                ));
        }

        return nodePropertyValues;
    }

}
