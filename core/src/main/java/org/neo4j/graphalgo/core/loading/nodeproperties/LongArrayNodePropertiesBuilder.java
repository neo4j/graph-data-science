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
package org.neo4j.graphalgo.core.loading.nodeproperties;

import org.neo4j.graphalgo.api.DefaultValue;
import org.neo4j.graphalgo.api.nodeproperties.LongArrayNodeProperties;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;
import org.neo4j.graphalgo.utils.ValueConversion;
import org.neo4j.values.storable.Value;

public class LongArrayNodePropertiesBuilder extends InnerNodePropertiesBuilder {

    private final HugeObjectArray<long[]> objectArray;
    private final DefaultValue defaultValue;

    public LongArrayNodePropertiesBuilder(long nodeCount, DefaultValue defaultValue, AllocationTracker tracker) {
        validateDefaultValueType(defaultValue, DefaultValue::longArrayValue);

        this.defaultValue = defaultValue;
        this.objectArray = HugeObjectArray.newArray(long[].class, nodeCount, tracker);
    }

    public void set(long nodeId, long[] value) {
        objectArray.set(nodeId, value);
    }

    @Override
    protected Class<?> valueClass() {
        return long[].class;
    }

    @Override
    public void setValue(long nodeId, Value value) {
        objectArray.set(nodeId, ValueConversion.getLongArray(value));
    }

    public void setValue(long nodeId, long[] value) {
        objectArray.set(nodeId, value);
    }

    @Override
    public LongArrayNodeProperties build(long size) {
        return new LongArrayStoreNodeProperties(objectArray, defaultValue, size);
    }

    static class LongArrayStoreNodeProperties implements LongArrayNodeProperties {
        private final HugeObjectArray<long[]> propertyValues;
        private final DefaultValue defaultValue;
        private final long size;

        LongArrayStoreNodeProperties(
            HugeObjectArray<long[]> propertyValues,
            DefaultValue defaultValue,
            long size
        ) {
            this.propertyValues = propertyValues;
            this.defaultValue = defaultValue;
            this.size = size;
        }

        @Override
        public long[] longArrayValue(long nodeId) {
            long[] data = propertyValues.get(nodeId);
            if (data == null) {
                return defaultValue.longArrayValue();
            }
            return data;
        }

        @Override
        public long size() {
            return size;
        }
    }
}
