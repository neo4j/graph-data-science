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
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.api.PartialIdMap;
import org.neo4j.gds.api.properties.nodes.LongArrayNodePropertyValues;
import org.neo4j.gds.collections.HugeSparseLongArrayArray;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.utils.Neo4jValueConversion;
import org.neo4j.values.storable.Value;

import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class LongArrayNodePropertiesBuilder implements InnerNodePropertiesBuilder {

    private final HugeSparseLongArrayArray.Builder builder;
    private final long[] defaultValue;
    private final int concurrency;

    public LongArrayNodePropertiesBuilder(
        DefaultValue defaultValue,
        int concurrency
    ) {
        this.defaultValue = defaultValue.longArrayValue();
        this.concurrency = concurrency;
        this.builder = HugeSparseLongArrayArray.builder(this.defaultValue);
    }

    public void set(long neoNodeId, long[] value) {
        builder.set(neoNodeId, value);
    }

    @Override
    public void setValue(long neoNodeId, Value value) {
        set(neoNodeId, Neo4jValueConversion.getLongArray(value));
    }

    public void setValue(long nodeId, long[] value) {
        builder.set(nodeId, value);
    }

    @Override
    public LongArrayNodePropertyValues build(long size, PartialIdMap idMap, long highestOriginalId) {
        var propertiesByNeoIds = builder.build();

        var propertiesByMappedIdsBuilder = HugeSparseLongArrayArray.builder(
            defaultValue
        );

        var drainingIterator = propertiesByNeoIds.drainingIterator();

        var tasks = IntStream.range(0, concurrency).mapToObj(threadId -> (Runnable) () -> {
            var batch = drainingIterator.drainingBatch();

            while (drainingIterator.next(batch)) {
                var page = batch.page;
                var offset = batch.offset;
                var end = Math.min(offset + page.length, highestOriginalId + 1) - offset;

                for (int pageIndex = 0; pageIndex < end; pageIndex++) {
                    var neoId = offset + pageIndex;
                    var mappedId = idMap.toMappedNodeId(neoId);
                    if (mappedId == IdMap.NOT_FOUND) {
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

        return new LongArrayStoreNodePropertyValues(propertyValues, size);
    }

    static class LongArrayStoreNodePropertyValues implements LongArrayNodePropertyValues {
        private final HugeSparseLongArrayArray propertyValues;
        private final long size;

        LongArrayStoreNodePropertyValues(
            HugeSparseLongArrayArray propertyValues,
            long size
        ) {
            this.propertyValues = propertyValues;
            this.size = size;
        }

        @Override
        public long[] longArrayValue(long nodeId) {
            return propertyValues.get(nodeId);
        }

        @Override
        public long valuesStored() {
            // FIXME this does not actually contain the number of stored values in the sparse array
            return size;
        }

        @Override
        public long maxIndex() {
            return size;
        }
    }
}
