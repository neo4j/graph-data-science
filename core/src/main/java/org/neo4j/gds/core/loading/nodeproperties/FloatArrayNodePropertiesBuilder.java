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
import org.neo4j.gds.api.properties.nodes.FloatArrayNodePropertyValues;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.collections.HugeSparseFloatArrayArray;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.utils.Neo4jValueConversion;
import org.neo4j.values.storable.Value;

import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class FloatArrayNodePropertiesBuilder extends InnerNodePropertiesBuilder {

    private final HugeSparseFloatArrayArray.Builder builder;
    private final float[] defaultValue;
    private final int concurrency;

    public FloatArrayNodePropertiesBuilder(
        DefaultValue defaultValue,
        int concurrency
    ) {
        this.concurrency = concurrency;
        this.defaultValue = defaultValue.floatArrayValue();
        this.builder = HugeSparseFloatArrayArray.builder(this.defaultValue);
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
    public NodePropertyValues buildDirect(long size) {
        return new FloatArrayStoreNodePropertyValues(builder.build(), size);
    }

    @Override
    public FloatArrayNodePropertyValues build(long size, IdMap idMap) {
        var propertiesByNeoIds = builder.build();

        var propertiesByMappedIdsBuilder = HugeSparseFloatArrayArray.builder(
            defaultValue
        );

        var drainingIterator = propertiesByNeoIds.drainingIterator();

        var tasks = IntStream.range(0, concurrency).mapToObj(threadId -> (Runnable) () -> {
            var batch = drainingIterator.drainingBatch();

            while (drainingIterator.next(batch)) {
                var page = batch.page;
                var offset = batch.offset;
                var end = Math.min(offset + page.length, idMap.highestOriginalId() + 1) - offset;

                for (int pageIndex = 0; pageIndex < end; pageIndex++) {
                    var neoId = offset + pageIndex;
                    var mappedId = idMap.toMappedNodeId(neoId);
                    if (mappedId == IdMap.NOT_FOUND) {
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

        return new FloatArrayStoreNodePropertyValues(propertyValues, size);
    }

    static class FloatArrayStoreNodePropertyValues implements FloatArrayNodePropertyValues {
        private final HugeSparseFloatArrayArray propertyValues;
        private final long size;

        FloatArrayStoreNodePropertyValues(
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
