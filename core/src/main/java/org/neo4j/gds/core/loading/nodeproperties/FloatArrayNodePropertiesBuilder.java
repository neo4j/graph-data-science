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
import org.neo4j.gds.api.nodeproperties.FloatArrayNodeProperties;
import org.neo4j.gds.collections.HugeSparseFloatArrayArray;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.utils.Neo4jValueConversion;
import org.neo4j.values.storable.Value;

public class FloatArrayNodePropertiesBuilder extends InnerNodePropertiesBuilder {

    private final HugeSparseFloatArrayArray.Builder builder;
    private final DefaultValue defaultValue;
    private final AllocationTracker allocationTracker;
    private final int concurrency;

    public FloatArrayNodePropertiesBuilder(DefaultValue defaultValue, AllocationTracker allocationTracker, int concurrency) {
        this.allocationTracker = allocationTracker;
        this.concurrency = concurrency;
        // validate defaultValue is a float array
        defaultValue.floatArrayValue();

        this.defaultValue = defaultValue;
        this.builder = HugeSparseFloatArrayArray.builder(defaultValue.floatArrayValue(), allocationTracker::add);
    }

    public void set(long neoNodeId, float[] value) {
        builder.set(neoNodeId, value);
    }

    @Override
    protected Class<?> valueClass() {
        return float[].class;
    }

    @Override
    public void setValue(long neoNodeId, Value value) {
        set(neoNodeId, Neo4jValueConversion.getFloatArray(value));
    }

    @Override
    public FloatArrayNodeProperties build(long size, NodeMapping nodeMapping) {
        var propertiesByNeoIds = builder.build();

        var propertiesByMappedIdsBuilder = HugeSparseFloatArrayArray.builder(
            defaultValue.floatArrayValue(),
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

        return new FloatArrayStoreNodeProperties(propertyValues, size);
    }

    static class FloatArrayStoreNodeProperties implements FloatArrayNodeProperties {
        private final HugeSparseFloatArrayArray propertyValues;
        private final long size;

        FloatArrayStoreNodeProperties(
            HugeSparseFloatArrayArray propertyValues,
            long size
        ) {
            this.propertyValues = propertyValues;
            this.size = size;
        }

        @Override
        public float[] floatArrayValue(long nodeId) {
            return propertyValues.get(nodeId);
        }

        @Override
        public long size() {
            return size;
        }
    }
}
