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
import org.neo4j.gds.api.NodeIdMapper;
import org.neo4j.gds.api.properties.nodes.FloatArrayNodePropertyValues;
import org.neo4j.gds.collections.hsa.HugeSparseFloatArrayArray;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.utils.GdsNeo4jValueConversion;
import org.neo4j.gds.values.GdsValue;

import java.util.Arrays;

public class FloatArrayNodePropertiesBuilder implements InnerNodePropertiesBuilder {

    private final HugeSparseFloatArrayArray.Builder builder;
    private final float[] defaultValue;
    private final Concurrency concurrency;

    public FloatArrayNodePropertiesBuilder(
        DefaultValue defaultValue,
        Concurrency concurrency
    ) {
        this.concurrency = concurrency;
        this.defaultValue = defaultValue.floatArrayValue();
        this.builder = HugeSparseFloatArrayArray.builder(this.defaultValue);
    }

    public void set(long neoNodeId, float[] value) {
        builder.set(neoNodeId, value);
    }

    @Override
    public void setValue(long neoNodeId, GdsValue value) {
        set(neoNodeId, GdsNeo4jValueConversion.getFloatArray(value));
    }

    @Override
    public FloatArrayNodePropertyValues build(long size, NodeIdMapper toMappedNodeIdFn, long highestOriginalId) {
        if (toMappedNodeIdFn == NodeIdMapper.IDENTITY) {
            // Values are already keyed by the internal id, so the source array can be reused
            return buildWithoutMapping(size);
        }
        return buildWithMapping(size, toMappedNodeIdFn, highestOriginalId);
    }

    private FloatArrayStoreNodePropertyValues buildWithoutMapping(long size) {
        return new FloatArrayStoreNodePropertyValues(builder.build(), size);
    }

    private FloatArrayStoreNodePropertyValues buildWithMapping(
        long size,
        NodeIdMapper toMappedNodeIdFn,
        long highestOriginalId
    ) {
        var propertiesByMappedIdsBuilder = HugeSparseFloatArrayArray.builder(defaultValue);

        InnerNodePropertiesBuilder.remapToInternalIds(
            builder.build().drainingIterator(),
            (value, mappedId) -> propertiesByMappedIdsBuilder.set(mappedId, value),
            value -> value == null || Arrays.equals(value, defaultValue),
            toMappedNodeIdFn,
            highestOriginalId,
            concurrency
        );

        return new FloatArrayStoreNodePropertyValues(propertiesByMappedIdsBuilder.build(), size);
    }

    static class FloatArrayStoreNodePropertyValues implements FloatArrayNodePropertyValues {
        private final HugeSparseFloatArrayArray propertyValues;
        private final long size;

        FloatArrayStoreNodePropertyValues(
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
        public long nodeCount() {
            return size;
        }

        @Override
        public boolean hasValue(long nodeId) {
            return propertyValues.contains(nodeId);
        }
    }
}
