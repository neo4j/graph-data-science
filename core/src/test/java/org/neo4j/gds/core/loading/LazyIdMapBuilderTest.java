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
package org.neo4j.gds.core.loading;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.PropertyState;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.loading.construction.NodeLabelTokens;
import org.neo4j.gds.core.utils.partition.PartitionUtils;

import java.util.Collections;
import java.util.Optional;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

class LazyIdMapBuilderTest {

    @Test
    void parallelAddDuplicateNodes() {
        int concurrency = 8;
        long idCount = 100_000;
        var rng = new Random(42);

        var lazyIdMapBuilder = new LazyIdMapBuilder(concurrency, false, false, PropertyState.PERSISTENT);

        var idList = LongStream.rangeClosed(1, idCount)
            .flatMap(id -> rng.nextBoolean() ? LongStream.of(id, id) : LongStream.of(id))
            .limit(idCount)
            .boxed()
            .collect(Collectors.toList());

        // shuffle to spread duplicates across the whole range
        Collections.shuffle(idList);

        var idArray = idList.stream().mapToLong(l -> l).toArray();

        var tasks = PartitionUtils.rangePartition(concurrency, idCount, partition -> (Runnable) () -> {
            int start = (int) partition.startNode();
            int end = (int) (start + partition.nodeCount());
            for (int i = start; i < end; i++) {
                long originalId = idArray[i];
                // We potentially insert the same original id from multiple threads.
                // This MUST not lead to new intermediate ids generated internally.
                lazyIdMapBuilder.addNode(originalId, NodeLabelTokens.empty());
            }

        }, Optional.empty());

        ParallelUtil.run(tasks, Pools.DEFAULT);

        var highLimitIdMap = lazyIdMapBuilder.build().idMap();

        Assertions.assertThat(highLimitIdMap.nodeCount()).isLessThan(idCount);

        for (int internalNodeId = 0; internalNodeId < highLimitIdMap.nodeCount(); internalNodeId++) {
            Assertions
                .assertThat(highLimitIdMap.toOriginalNodeId(internalNodeId))
                .as("Internal node id %s is not mapped", internalNodeId)
                .isNotEqualTo(0);
        }
    }

}
