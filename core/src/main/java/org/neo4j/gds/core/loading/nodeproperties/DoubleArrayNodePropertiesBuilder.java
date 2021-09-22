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
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.utils.ValueConversion;
import org.neo4j.values.storable.Value;

public class DoubleArrayNodePropertiesBuilder extends InnerNodePropertiesBuilder {

    private final HugeObjectArray<double[]> objectArray;
    private final DefaultValue defaultValue;

    public DoubleArrayNodePropertiesBuilder(long nodeCount, DefaultValue defaultValue, AllocationTracker allocationTracker) {
        // validate defaultValue is a double array
        defaultValue.doubleArrayValue();

        this.objectArray = HugeObjectArray.newArray(double[].class, nodeCount, allocationTracker);
        this.defaultValue = defaultValue;
    }

    public void set(long nodeId, double[] value) {
        objectArray.set(nodeId, value);
    }

    @Override
    protected Class<?> valueClass() {
        return double[].class;
    }

    @Override
    public void setValue(long nodeId, long neoNodeId, Value value) {
        objectArray.set(nodeId, ValueConversion.getDoubleArray(value));
    }

    @Override
    public DoubleArrayNodeProperties build(long size, NodeMapping nodeMapping) {
        return new DoubleArrayStoreNodeProperties(objectArray, defaultValue, size);
    }

    static class DoubleArrayStoreNodeProperties implements DoubleArrayNodeProperties {
        private final HugeObjectArray<double[]> propertyValues;
        private final double[] defaultDoubleArray;
        private final long size;

        DoubleArrayStoreNodeProperties(
            HugeObjectArray<double[]> propertyValues,
            DefaultValue defaultValue,
            long size
        ) {
            this.propertyValues = propertyValues;
            this.defaultDoubleArray = defaultValue.doubleArrayValue();
            this.size = size;
        }

        @Override
        public double[] doubleArrayValue(long nodeId) {
            double[] data = propertyValues.get(nodeId);
            if (data == null) {
                return defaultDoubleArray;
            }
            return data;
        }

        @Override
        public long size() {
            return size;
        }
    }
}
