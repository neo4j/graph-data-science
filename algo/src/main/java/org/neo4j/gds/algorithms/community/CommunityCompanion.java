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
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.api.properties.nodes.LongArrayNodePropertyValues;
import org.neo4j.gds.api.properties.nodes.LongNodePropertyValues;
import org.neo4j.gds.api.properties.nodes.NodeProperty;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.collections.hsa.HugeSparseLongArray;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.result.CommunityStatistics;
import org.neo4j.values.storable.LongValue;
import org.neo4j.values.storable.Value;

import java.util.ArrayList;
import java.util.List;
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

        if (isIncremental && resultProperty.equals(seedProperty)) {
            nodeProperties = LongIfChangedNodePropertyValues.of(seedPropertySupplier.get(), nodeProperties);
        }

        if (consecutiveIds) {
            return new ConsecutiveLongNodePropertyValues(nodeProperties);
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

    static List<List<Double>> arrayMatrixToListMatrix(boolean shouldCompute, double[][] matrix) {
        if (shouldCompute) {
            var result = new ArrayList<List<Double>>();

            for (double[] row : matrix) {
                List<Double> rowList = new ArrayList<>();
                result.add(rowList);
                for (double column : row)
                    rowList.add(column);
            }
            return result;
        }
        return null;
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

    public static NodePropertyValues extractSeedingNodePropertyValues(Graph graph, String seedingProperty) {

        var nodePropertyValues = graph.nodeProperties(seedingProperty);
        if (nodePropertyValues == null)
            return null;

        if (nodePropertyValues.valueType() != ValueType.LONG) {
            throw new IllegalArgumentException(
                formatWithLocale(
                    " Provided seeding property `%s`  does not comprise exclusively of long values",
                    seedingProperty
                ));
        }

        return nodePropertyValues;
    }

}
