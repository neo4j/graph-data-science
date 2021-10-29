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
import org.neo4j.gds.api.IdMapping;
import org.neo4j.gds.api.NodeMapping;
import org.neo4j.gds.api.nodeproperties.FloatArrayNodeProperties;
import org.neo4j.gds.collections.HugeSparseFloatArrayArray;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.utils.Neo4jValueConversion;
import org.neo4j.values.storable.Value;

import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class FloatArrayNodePropertiesBuilder extends InnerNodePropertiesBuilder {

    private final HugeSparseFloatArrayArray.Builder builder;
    private final float[] defaultValue;
    private final AllocationTracker allocationTracker;
    private final int concurrency;

    public FloatArrayNodePropertiesBuilder(DefaultValue defaultValue, AllocationTracker allocationTracker, int concurrency) {
        this.allocationTracker = allocationTracker;
        this.concurrency = concurrency;
        this.defaultValue = defaultValue.floatArrayValue();
        this.builder = HugeSparseFloatArrayArray.builder(this.defaultValue, allocationTracker::add);
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
            defaultValue,
            allocationTracker::add
        );

        var drainingIterator = propertiesByNeoIds.drainingIterator();

        var tasks = IntStream.range(0, concurrency).mapToObj(threadId -> (Runnable) () -> {
            var batch = drainingIterator.drainingBatch();

            while (drainingIterator.next(batch)) {
                var page = batch.page;
                var offset = batch.offset;
                var end = Math.min(offset + page.length, nodeMapping.highestNeoId() + 1) - offset;

                for (int pageIndex = 0; pageIndex < end; pageIndex++) {
                    var neoId = offset + pageIndex;
                    var mappedId = nodeMapping.toMappedNodeId(neoId);
                    if (mappedId == IdMapping.NOT_FOUND) {
                        continue;
                    }
                    var value = page[pageIndex];
                    if (value == null || Arrays.equals(value, defaultValue)) {
                        continue;
                    }
                    propertiesByMappedIdsBuilder.set(mappedId, value);
                }
            }
        }).collect(Collectors.toList());

        ParallelUtil.run(tasks, Pools.DEFAULT);
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
