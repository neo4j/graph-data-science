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
package org.neo4j.graphalgo;

import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.api.nodeproperties.ConsecutiveLongNodeProperties;
import org.neo4j.graphalgo.api.nodeproperties.LongIfChangedNodeProperties;
import org.neo4j.graphalgo.api.nodeproperties.LongNodeProperties;
import org.neo4j.graphalgo.config.AlgoBaseConfig;
import org.neo4j.graphalgo.config.CommunitySizeConfig;
import org.neo4j.graphalgo.config.ConsecutiveIdsConfig;
import org.neo4j.graphalgo.config.SeedConfig;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeSparseLongArray;
import org.neo4j.graphalgo.core.utils.statistics.CommunityStatistics;
import org.neo4j.values.storable.LongValue;
import org.neo4j.values.storable.Value;

public final class CommunityProcCompanion {

    private CommunityProcCompanion() {}

    public static <ALGO extends Algorithm<ALGO, RESULT>, RESULT, CONFIG extends AlgoBaseConfig & SeedConfig & ConsecutiveIdsConfig> NodeProperties nodeProperties(
        AlgoBaseProc.ComputationResult<ALGO, RESULT, CONFIG> computationResult,
        String resultProperty,
        LongNodeProperties nodeProperties,
        AllocationTracker tracker
    ) {
        var config = computationResult.config();
        var graphStore = computationResult.graphStore();

        var consecutiveIds = config.consecutiveIds();
        var isIncremental = config.isIncremental();
        var seedProperty = config.seedProperty();
        var resultPropertyEqualsSeedProperty = isIncremental && resultProperty.equals(seedProperty);

        LongNodeProperties result;

        if (resultPropertyEqualsSeedProperty && !consecutiveIds) {
            result = LongIfChangedNodeProperties.of(graphStore, seedProperty, nodeProperties);
        } else if (consecutiveIds && !isIncremental) {
            result = new ConsecutiveLongNodeProperties(
                nodeProperties,
                computationResult.graph().nodeCount(),
                tracker
            );
        } else {
            result = nodeProperties;
        }

        if (config instanceof CommunitySizeConfig) {
            var csc = (CommunitySizeConfig) config;
            if (csc.hasMinCommunitySize()) {
                var communitySizes = CommunityStatistics.communitySizes(
                    result.size(),
                    result::longValue,
                    Pools.DEFAULT,
                    config.concurrency(),
                    tracker
                );
                result = new CommunitySizeFilter(result, communitySizes, csc.minCommunitySize());
            }
        }

        return result;
    }

    private static class CommunitySizeFilter implements LongNodeProperties {

        private final LongNodeProperties properties;

        private final HugeSparseLongArray communitySizes;

        private final long minCommunitySize;

        CommunitySizeFilter(LongNodeProperties properties, HugeSparseLongArray communitySizes, long minCommunitySize) {
            this.properties = properties;
            this.communitySizes = communitySizes;
            this.minCommunitySize = minCommunitySize;
        }

        @Override
        public long size() {
            return properties.size();
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
