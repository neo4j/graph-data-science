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
import org.neo4j.graphalgo.api.nodeproperties.DoubleNodeProperties;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeSparseLongArray;
import org.neo4j.graphalgo.utils.ValueConversion;
import org.neo4j.values.storable.Value;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.OptionalDouble;

public class DoubleNodePropertiesBuilder extends InnerNodePropertiesBuilder {

    // Value is changed with a VarHandle and needs to be non final for that
    // even though our favourite IDE/OS doesn't pick that up
    @SuppressWarnings({"FieldMayBeFinal", "FieldCanBeLocal"})
    private volatile double maxValue;

    private static final VarHandle MAX_VALUE;

    static {
        VarHandle maxValueHandle;
        try {
            maxValueHandle = MethodHandles
                .lookup()
                .findVarHandle(DoubleNodePropertiesBuilder.class, "maxValue", double.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
        MAX_VALUE = maxValueHandle;
    }

    private final HugeSparseLongArray.Builder valuesBuilder;

    public DoubleNodePropertiesBuilder(long nodeCount, DefaultValue defaultValue, AllocationTracker tracker) {
        this.maxValue = Double.NEGATIVE_INFINITY;
        this.valuesBuilder = HugeSparseLongArray.builder(nodeCount, Double.doubleToLongBits(defaultValue.doubleValue()), tracker);
    }

    public void set(long nodeId, double value) {
        valuesBuilder.set(nodeId, Double.doubleToLongBits(value));
        updateMaxValue(value);
    }

    @Override
    protected Class<?> valueClass() {
        return double.class;
    }

    @Override
    public void setValue(long nodeId, Value value) {
        double doubleValue = ValueConversion.getDoubleValue(value);
        valuesBuilder.set(nodeId, Double.doubleToLongBits(doubleValue));
        updateMaxValue(doubleValue);
    }

    @Override
    public DoubleNodeProperties build(long size) {
        HugeSparseLongArray propertyValues = valuesBuilder.build();
        var maybeMaxValue = size > 0
            ? OptionalDouble.of((double) MAX_VALUE.getVolatile(DoubleNodePropertiesBuilder.this))
            : OptionalDouble.empty();

        return new DoubleStoreNodeProperties(propertyValues, size, maybeMaxValue);
    }

    private void updateMaxValue(double value) {
        // We are basically doing the equivalent of
        //    this.maxValue = Math.max(value, this.maxValue);
        // but we need to deal with ordering and atomicity issues when this
        // is called concurrently.

        // First try to read without ordering guarantees
        // this value could be an outdated one, but any other value
        // would be strictly greater than this one, so if our `value`
        // is already smaller, we don't need to write to the maxValue field.
        // Think of this as the initial read in a double-checked locking read.
        // Also, we can't use getPlain here, since that one has no atomicity
        // guarantees for doubles, so we need to read at least with opaque semantics
        // to make sure, that we don't read doubles that are currently being updated
        // where some bits are from the old value and others from the new value.
        double currentMax = (double) MAX_VALUE.getOpaque(this);
        if (currentMax >= value) {
            return;
        }

        // start a CAS loop;
        // if the currentMax was outdated, we will get the new value after the first failed CAS
        // otherwise we might already be done
        while (currentMax < value) {
            double newMax = (double) MAX_VALUE.compareAndExchange(this, currentMax, value);
            if (newMax == currentMax) {
                // CAS success, we are now the maxValue
                return;
            }
            // update local copy and try again
            currentMax = newMax;
        }
    }

    static class DoubleStoreNodeProperties implements DoubleNodeProperties {
        private final HugeSparseLongArray propertyValues;
        private final long size;
        private final OptionalDouble maxValue;

        DoubleStoreNodeProperties(HugeSparseLongArray propertyValues, long size, OptionalDouble maxValue) {
            this.propertyValues = propertyValues;
            this.size = size;
            this.maxValue = maxValue;
        }

        @Override
        public double doubleValue(long nodeId) {
            return Double.longBitsToDouble(propertyValues.get(nodeId));
        }

        @Override
        public OptionalDouble getMaxDoublePropertyValue() {
            return maxValue;
        }

        @Override
        public long size() {
            return size;
        }
    }
}
