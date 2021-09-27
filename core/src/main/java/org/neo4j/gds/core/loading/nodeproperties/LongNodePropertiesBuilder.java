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
import org.neo4j.gds.api.NodeProperties;
import org.neo4j.gds.api.nodeproperties.LongNodeProperties;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.paged.HugeSparseLongArray;
import org.neo4j.gds.utils.GdsFeatureToggles;
import org.neo4j.gds.utils.ValueConversion;
import org.neo4j.values.storable.Value;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.OptionalLong;

public abstract class LongNodePropertiesBuilder extends InnerNodePropertiesBuilder {

    // Value is changed with a VarHandle and needs to be non final for that
    // even though our favourite IDE/OS doesn't pick that up
    @SuppressWarnings({"FieldMayBeFinal", "FieldCanBeLocal"})
    private volatile long maxValue;

    private static final VarHandle MAX_VALUE;
    protected final AllocationTracker allocationTracker;

    static {
        VarHandle maxValueHandle;
        try {
            maxValueHandle = MethodHandles
                .lookup()
                .findVarHandle(LongNodePropertiesBuilder.class, "maxValue", long.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
        MAX_VALUE = maxValueHandle;
    }

    private LongNodePropertiesBuilder(AllocationTracker allocationTracker) {
        this.allocationTracker = allocationTracker;
        this.maxValue = Long.MIN_VALUE;
    }

    public static LongNodePropertiesBuilder of(
        long nodeCount,
        DefaultValue defaultValue,
        AllocationTracker allocationTracker,
        int concurrency
    ) {
        return GdsFeatureToggles.USE_NEO_IDS_FOR_PROPERTY_IMPORT.isEnabled()
            ? sparse(defaultValue, allocationTracker, concurrency)
            : dense(nodeCount, defaultValue, allocationTracker);
    }

    public static LongNodePropertiesBuilder sparse(
        DefaultValue defaultValue,
        AllocationTracker allocationTracker,
        int concurrency
    ) {
        var defaultLongValue = defaultValue.longValue();
        var builder = HugeSparseLongArray.GrowingBuilder.create(defaultLongValue, allocationTracker);
        return new SparseLongNodePropertiesBuilder(builder, defaultLongValue, concurrency, allocationTracker);
    }

    public static LongNodePropertiesBuilder dense(
        long nodeCount,
        DefaultValue defaultValue,
        AllocationTracker allocationTracker
    ) {
        var builder = HugeSparseLongArray.builder(nodeCount, defaultValue.longValue(), allocationTracker);
        return new DenseLongNodePropertiesBuilder(builder, allocationTracker);
    }

    public abstract void set(long nodeId, long neoNodeId, long value);

    abstract HugeSparseLongArray buildInner(NodeMapping nodeMapping);

    @Override
    protected Class<?> valueClass() {
        return long.class;
    }

    @Override
    public void setValue(long nodeId, long neoNodeId, Value value) {
        var longValue = ValueConversion.getLongValue(value);
        set(nodeId, neoNodeId, longValue);
        updateMaxValue(longValue);
    }

    @Override
    public NodeProperties build(long size, NodeMapping nodeMapping) {
        HugeSparseLongArray propertyValues = buildInner(nodeMapping);

        var maybeMaxValue = size > 0
            ? OptionalLong.of((long) MAX_VALUE.getVolatile(LongNodePropertiesBuilder.this))
            : OptionalLong.empty();

        return new LongStoreNodeProperties(propertyValues, size, maybeMaxValue);
    }

    private void updateMaxValue(long value) {
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
        long currentMax = (long) MAX_VALUE.getOpaque(this);
        if (currentMax >= value) {
            return;
        }

        // start a CAS loop;
        // if the currentMax was outdated, we will get the new value after the first failed CAS
        // otherwise we might already be done
        while (currentMax < value) {
            long newMax = (long) MAX_VALUE.compareAndExchange(this, currentMax, value);
            if (newMax == currentMax) {
                // CAS success, we are now the maxValue
                return;
            }
            // update local copy and try again
            currentMax = newMax;
        }
    }

    static class LongStoreNodeProperties implements LongNodeProperties {
        private final HugeSparseLongArray propertyValues;
        private final long size;
        private final OptionalLong maxValue;

        LongStoreNodeProperties(HugeSparseLongArray propertyValues, long size, OptionalLong maxValue) {
            this.propertyValues = propertyValues;
            this.size = size;
            this.maxValue = maxValue;
        }

        @Override
        public long longValue(long nodeId) {
            return propertyValues.get(nodeId);
        }

        @Override
        public OptionalLong getMaxLongPropertyValue() {
            return maxValue;
        }

        @Override
        public long size() {
            return size;
        }
    }

    private static final class SparseLongNodePropertiesBuilder extends LongNodePropertiesBuilder {

        private final HugeSparseLongArray.GrowingBuilder builder;
        private final long defaultValue;
        private final int concurrency;

        private SparseLongNodePropertiesBuilder(
            HugeSparseLongArray.GrowingBuilder builder,
            long defaultValue,
            int concurrency,
            AllocationTracker allocationTracker
        ) {
            super(allocationTracker);
            this.builder = builder;
            this.defaultValue = defaultValue;
            this.concurrency = concurrency;
        }

        @Override
        public void set(long nodeId, long neoNodeId, long value) {
            builder.set(neoNodeId, value);
        }

        @Override
        HugeSparseLongArray buildInner(NodeMapping nodeMapping) {
            var propertiesByNeoIds = builder.build();
            var propertiesByMappedIdsBuilder = HugeSparseLongArray.builder(
                nodeMapping.nodeCount(),
                defaultValue,
                allocationTracker
            );

            ParallelUtil.parallelForEachNode(
                nodeMapping.nodeCount(),
                concurrency,
                mappedId -> {
                    var neoId = nodeMapping.toOriginalNodeId(mappedId);
                    var value = propertiesByNeoIds.get(neoId);
                    propertiesByMappedIdsBuilder.set(mappedId, value);
                }
            );

            return propertiesByMappedIdsBuilder.build();
        }
    }

    private static final class DenseLongNodePropertiesBuilder extends LongNodePropertiesBuilder {
        private final HugeSparseLongArray.Builder builder;

        DenseLongNodePropertiesBuilder(
            HugeSparseLongArray.Builder builder,
            AllocationTracker allocationTracker
        ) {
            super(allocationTracker);
            this.builder = builder;
        }

        @Override
        public void set(long nodeId, long neoNodeId, long value) {
            builder.set(nodeId, value);
        }

        @Override
        HugeSparseLongArray buildInner(NodeMapping nodeMapping) {
            return builder.build();
        }
    }
}
