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

import org.neo4j.gds.api.NodeIdMapper;
import org.neo4j.gds.api.nodes.IdMap;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.collections.DrainingIterator;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.values.GdsValue;

import java.util.function.ObjLongConsumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public interface InnerNodePropertiesBuilder {

    void setValue(long neoNodeId, GdsValue value);

    /**
     * Builds the underlying node properties and performs a remapping
     * to the internal id space using the given mapping function.
     */
    NodePropertyValues build(long size, NodeIdMapper toMappedNodeIdFn, long highestOriginalId);

    /**
     * Drains values keyed by their original id into {@code sink} keyed by their mapped id, skipping
     * values that are absent or equal to the property's default. Only usable where a page is an array
     * of the value type ({@code T[]}), which excludes the scalar builders: their page is a primitive
     * array, which no {@code T[]} can denote.
     */
    static <T> void remapToInternalIds(
        DrainingIterator<T[]> valuesByOriginalId,
        ObjLongConsumer<T> sink,
        Predicate<T> skipValue,
        NodeIdMapper toMappedNodeIdFn,
        long highestOriginalId,
        Concurrency concurrency
    ) {
        var tasks = IntStream.range(0, concurrency.value()).mapToObj(threadId -> (Runnable) () -> {
            var batch = valuesByOriginalId.drainingBatch();

            while (valuesByOriginalId.next(batch)) {
                var page = batch.page;
                var offset = batch.offset;
                var end = Math.min(offset + page.length, highestOriginalId + 1) - offset;

                for (int pageIndex = 0; pageIndex < end; pageIndex++) {
                    var neoId = offset + pageIndex;
                    var mappedId = toMappedNodeIdFn.map(neoId);
                    if (mappedId == IdMap.NOT_FOUND) {
                        continue;
                    }
                    var value = page[pageIndex];
                    if (skipValue.test(value)) {
                        continue;
                    }
                    sink.accept(value, mappedId);
                }
            }
        }).collect(Collectors.toList());

        ParallelUtil.run(tasks, DefaultPool.INSTANCE);
    }
}
