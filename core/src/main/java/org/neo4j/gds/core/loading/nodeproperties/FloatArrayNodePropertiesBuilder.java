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
import org.neo4j.gds.api.nodeproperties.FloatArrayNodeProperties;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.utils.ValueConversion;
import org.neo4j.values.storable.Value;

public class FloatArrayNodePropertiesBuilder extends InnerNodePropertiesBuilder {

    private final HugeObjectArray<float[]> objectArray;
    private final DefaultValue defaultValue;

    public FloatArrayNodePropertiesBuilder(long nodeCount, DefaultValue defaultValue, AllocationTracker tracker) {
        // validate defaultValue is a float array
        defaultValue.floatArrayValue();

        this.defaultValue = defaultValue;
        this.objectArray = HugeObjectArray.newArray(float[].class, nodeCount, tracker);
    }

    public void set(long nodeId, float[] value) {
        objectArray.set(nodeId, value);
    }

    @Override
    protected Class<?> valueClass() {
        return float[].class;
    }

    @Override
    public void setValue(long nodeId, Value value) {
        objectArray.set(nodeId, ValueConversion.getFloatArray(value));
    }

    @Override
    public FloatArrayNodeProperties build(long size) {
        return new FloatArrayStoreNodeProperties(objectArray, defaultValue, size);
    }

    static class FloatArrayStoreNodeProperties implements FloatArrayNodeProperties {
        private final HugeObjectArray<float[]> propertyValues;
        private final DefaultValue defaultValue;
        private final long size;

        FloatArrayStoreNodeProperties(
            HugeObjectArray<float[]> propertyValues,
            DefaultValue defaultValue,
            long size
        ) {
            this.propertyValues = propertyValues;
            this.defaultValue = defaultValue;
            this.size = size;
        }

        @Override
        public float[] floatArrayValue(long nodeId) {
            float[] data = propertyValues.get(nodeId);
            if (data == null) {
                return defaultValue.floatArrayValue();
            }
            return data;
        }

        @Override
        public long size() {
            return size;
        }
    }
}
