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
import org.neo4j.gds.collections.HugeSparseDoubleArrayArray;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.utils.Neo4jValueConversion;
import org.neo4j.values.storable.Value;

import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class DoubleArrayNodePropertiesBuilder extends InnerNodePropertiesBuilder {

    private final HugeSparseDoubleArrayArray.Builder builder;
    private final double[] defaultValue;
    private final AllocationTracker allocationTracker;
    private final int concurrency;

    public DoubleArrayNodePropertiesBuilder(
        DefaultValue defaultValue,
        AllocationTracker allocationTracker,
        int concurrency
    ) {
        this.allocationTracker = allocationTracker;
        this.concurrency = concurrency;
        this.defaultValue = defaultValue.doubleArrayValue();
        this.builder = HugeSparseDoubleArrayArray.builder(
            this.defaultValue,
            allocationTracker::add
        );
    }

    public void set(long neoNodeId, double[] value) {
        builder.set(neoNodeId, value);
    }

    @Override
    protected Class<?> valueClass() {
        return double[].class;
    }

    @Override
    public void setValue(long neoNodeId, Value value) {
        set(neoNodeId, Neo4jValueConversion.getDoubleArray(value));
    }

    @Override
    public DoubleArrayNodeProperties build(long size, NodeMapping nodeMapping) {
        var propertiesByNeoIds = builder.build();

        var propertiesByMappedIdsBuilder = HugeSparseDoubleArrayArray.builder(
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
                    if (mappedId == NodeMapping.NOT_FOUND) {
                        continue;
                    }
                    var value = page[pageIndex];
                    if (value == null || (defaultValue != null && Arrays.equals(value, defaultValue))) {
                        continue;
                    }
                    propertiesByMappedIdsBuilder.set(mappedId, value);
                }
            }
        }).collect(Collectors.toList());
        ParallelUtil.run(tasks, Pools.DEFAULT);

        var propertyValues = propertiesByMappedIdsBuilder.build();

        return new DoubleArrayStoreNodeProperties(propertyValues, size);
    }

    static class DoubleArrayStoreNodeProperties implements DoubleArrayNodeProperties {
        private final HugeSparseDoubleArrayArray propertyValues;
        private final long size;

        DoubleArrayStoreNodeProperties(
            HugeSparseDoubleArrayArray propertyValues,
            long size
        ) {
            this.propertyValues = propertyValues;
            this.size = size;
        }

        @Override
        public double[] doubleArrayValue(long nodeId) {
            return propertyValues.get(nodeId);
        }

        @Override
        public long size() {
            return size;
        }
    }
}
