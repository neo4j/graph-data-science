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
package org.neo4j.gds.core.loading.nodeproperties;

import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.NodeMapping;
import org.neo4j.gds.api.nodeproperties.DoubleArrayNodeProperties;
import org.neo4j.gds.collections.HugeSparseDoubleArrayArray;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.utils.Neo4jValueConversion;
import org.neo4j.values.storable.Value;

public class DoubleArrayNodePropertiesBuilder extends InnerNodePropertiesBuilder {

    private final HugeSparseDoubleArrayArray.Builder builder;
    private final DefaultValue defaultValue;
    private final AllocationTracker allocationTracker;
    private final int concurrency;

    public DoubleArrayNodePropertiesBuilder(
        DefaultValue defaultValue,
        AllocationTracker allocationTracker,
        int concurrency
    ) {
        this.allocationTracker = allocationTracker;
        this.concurrency = concurrency;
        this.defaultValue = defaultValue;
        this.builder = HugeSparseDoubleArrayArray.growingBuilder(
            defaultValue.doubleArrayValue(),
            allocationTracker::add
        );
    }

    public void set(long neoNodeId, double[] value) {
        builder.set(neoNodeId, value);
    }

    @Override
    protected Class<?> valueClass() {
        return double[].class;
    }

    @Override
    public void setValue(long neoNodeId, Value value) {
        set(neoNodeId, Neo4jValueConversion.getDoubleArray(value));
    }

    @Override
    public DoubleArrayNodeProperties build(long size, NodeMapping nodeMapping) {
        var propertiesByNeoIds = builder.build();

        var propertiesByMappedIdsBuilder = HugeSparseDoubleArrayArray.growingBuilder(
            defaultValue.doubleArrayValue(),
            allocationTracker::add
        );

        ParallelUtil.parallelForEachNode(
            nodeMapping.nodeCount(),
            concurrency,
            mappedId -> {
                var neoId = nodeMapping.toOriginalNodeId(mappedId);
                if (propertiesByNeoIds.contains(neoId)) {
                    propertiesByMappedIdsBuilder.set(mappedId, propertiesByNeoIds.get(neoId));
                }
            }
        );

        var propertyValues = propertiesByMappedIdsBuilder.build();

        return new DoubleArrayStoreNodeProperties(propertyValues, size);
    }

    static class DoubleArrayStoreNodeProperties implements DoubleArrayNodeProperties {
        private final HugeSparseDoubleArrayArray propertyValues;
        private final long size;

        DoubleArrayStoreNodeProperties(
            HugeSparseDoubleArrayArray propertyValues,
            long size
        ) {
            this.propertyValues = propertyValues;
            this.size = size;
        }

        @Override
        public double[] doubleArrayValue(long nodeId) {
            return propertyValues.get(nodeId);
        }

        @Override
        public long size() {
            return size;
        }
    }
}
