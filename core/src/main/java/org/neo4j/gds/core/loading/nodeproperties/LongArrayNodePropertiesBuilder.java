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
import org.neo4j.gds.api.nodeproperties.LongArrayNodeProperties;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.utils.Neo4jValueConversion;
import org.neo4j.values.storable.Value;

public class LongArrayNodePropertiesBuilder extends InnerNodePropertiesBuilder {

    private final HugeObjectArray<long[]> objectArray;
    private final DefaultValue defaultValue;

    public LongArrayNodePropertiesBuilder(long nodeCount, DefaultValue defaultValue, AllocationTracker allocationTracker) {
        // validate defaultValue is a long array
        defaultValue.longArrayValue();

        this.defaultValue = defaultValue;
        this.objectArray = HugeObjectArray.newArray(long[].class, nodeCount, allocationTracker);
    }

    public void set(long nodeId, long[] value) {
        objectArray.set(nodeId, value);
    }

    @Override
    protected Class<?> valueClass() {
        return long[].class;
    }

    @Override
    public void setValue(long nodeId, long neoNodeId, Value value) {
        objectArray.set(nodeId, Neo4jValueConversion.getLongArray(value));
    }

    public void setValue(long nodeId, long[] value) {
        objectArray.set(nodeId, value);
    }

    @Override
    public LongArrayNodeProperties build(long size, NodeMapping nodeMapping) {
        return new LongArrayStoreNodeProperties(objectArray, defaultValue, size);
    }

    static class LongArrayStoreNodeProperties implements LongArrayNodeProperties {
        private final HugeObjectArray<long[]> propertyValues;
        private final long[] defaultLongArray;
        private final long size;

        LongArrayStoreNodeProperties(
            HugeObjectArray<long[]> propertyValues,
            DefaultValue defaultValue,
            long size
        ) {
            this.propertyValues = propertyValues;
            this.defaultLongArray= defaultValue.longArrayValue();
            this.size = size;
        }

        @Override
        public long[] longArrayValue(long nodeId) {
            long[] data = propertyValues.get(nodeId);
            if (data == null) {
                return defaultLongArray;
            }
            return data;
        }

        @Override
        public long size() {
            return size;
        }
    }
}
