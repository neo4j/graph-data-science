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
import org.neo4j.gds.api.properties.nodes.FloatVectorNodePropertyValues;
import org.neo4j.gds.collections.hsa.HugeSparseFloatArrayArray;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.utils.GdsNeo4jValueConversion;
import org.neo4j.gds.values.GdsValue;

import java.util.Arrays;

public class FloatVectorNodePropertiesBuilder implements InnerNodePropertiesBuilder {

    private final HugeSparseFloatArrayArray.Builder builder;
    private final float[] defaultValue;
    private final Concurrency concurrency;
    private int dimension;

    public FloatVectorNodePropertiesBuilder(
        DefaultValue defaultValue,
        Concurrency concurrency
    ) {
        this.concurrency = concurrency;
        this.defaultValue = defaultValue.floatArrayValue();
        this.builder = HugeSparseFloatArrayArray.builder(
            this.defaultValue
        );
        this.dimension = this.defaultValue != null ? this.defaultValue.length : -1;
    }

    public void set(long neoNodeId, float[] value) {
        builder.set(neoNodeId, value);
        if (value != null) {
            if (this.dimension != -1 && value.length != this.dimension) {
                throw new IllegalArgumentException("Vector dimension mismatch. Expected " + this.dimension + ", got " + value.length);
            }
            this.dimension = value.length;
        }
    }

    @Override
    public void setValue(long neoNodeId, GdsValue value) {
        set(neoNodeId, GdsNeo4jValueConversion.getFloatArray(value));
    }

    @Override
    public FloatVectorNodePropertyValues build(long size, NodeIdMapper toMappedNodeIdFn, long highestOriginalId) {
        if (toMappedNodeIdFn == NodeIdMapper.IDENTITY) {
            // Values are already keyed by the internal id, so the source array can be reused
            return buildWithoutMapping(size);
        }
        return buildWithMapping(size, toMappedNodeIdFn, highestOriginalId);
    }

    private FloatVectorStoreNodePropertyValues buildWithoutMapping(long size) {
        return new FloatVectorStoreNodePropertyValues(builder.build(), size, this.dimension);
    }

    private FloatVectorStoreNodePropertyValues buildWithMapping(
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

        return new FloatVectorStoreNodePropertyValues(propertiesByMappedIdsBuilder.build(), size, this.dimension);
    }

    static class FloatVectorStoreNodePropertyValues implements FloatVectorNodePropertyValues {
        private final HugeSparseFloatArrayArray propertyValues;
        private final long size;
        private final int dimension;

        FloatVectorStoreNodePropertyValues(
            HugeSparseFloatArrayArray propertyValues,
            long size,
            int dimension
        ) {
            this.propertyValues = propertyValues;
            this.size = size;
            this.dimension = dimension;
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

        @Override
        public int vectorDimension() {
            return dimension;
        }
    }
}
