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
package org.neo4j.gds.similarity.knn;

import com.carrotsearch.hppc.LongArrayList;
import net.jqwik.api.ForAll;
import net.jqwik.api.From;
import net.jqwik.api.Property;
import org.eclipse.collections.api.tuple.primitive.IntIntPair;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;

import java.util.SplittableRandom;
import java.util.stream.LongStream;
import java.util.stream.StreamSupport;

import static org.assertj.core.api.Assertions.assertThat;

class SplitOldAndNewNeighborsTest extends RandomNodeCountAndKValues {

    @Property(tries = 50)
    void name(
        @ForAll @From("n and k") IntIntPair nAndK
    ) {
        int nodeCount = nAndK.getOne();
        int k = nAndK.getTwo();
        int sampledK = k / 2;

        var allNeighbors = HugeObjectArray.newArray(
            NeighborList.class,
            nodeCount,
            AllocationTracker.empty()
        );

        SplittableRandom rng = new SplittableRandom();
        allNeighbors.setAll(nodeId -> {
            var neighbors = new NeighborList(k);
            LongStream.concat(
                LongStream.range(nodeId + 1, nodeCount),
                LongStream.range(0, nodeId)
            )
                .limit(k)
                .forEach(neighbor -> {
                    if (neighbor % 2 != 0) {
                        neighbor |= Long.MIN_VALUE;
                    }
                    neighbors.add(neighbor, 1.0, rng);
                });

            return neighbors;
        });

        var allOldNeighbors = HugeObjectArray.newArray(
            LongArrayList.class,
            nodeCount,
            AllocationTracker.empty()
        );
        var allNewNeighbors = HugeObjectArray.newArray(
            LongArrayList.class,
            nodeCount,
            AllocationTracker.empty()
        );

        var splitNeighbors = new SplitOldAndNewNeighbors(
            new SplittableRandom(),
            allNeighbors,
            allOldNeighbors,
            allNewNeighbors,
            sampledK,
            progressTracker
        );
        splitNeighbors.apply(0, nodeCount);

        var possibleNewNeighbors = LongStream.range(0, nodeCount).filter(n -> n % 2 == 0).toArray();
        var possibleOldNeighbors = LongStream.range(0, nodeCount).filter(n -> n % 2 != 0).toArray();

        for (int nodeId = 0; nodeId < nodeCount; nodeId++) {
            var oldNeighbors = allOldNeighbors.get(nodeId);
            if (oldNeighbors != null) {
                var neighbors = StreamSupport.stream(oldNeighbors.spliterator(), false)
                    .mapToLong(c -> c.value)
                    .toArray();
                assertThat(neighbors)
                    .hasSizeGreaterThanOrEqualTo(1)
                    .doesNotContain(nodeId)
                    .containsAnyOf(possibleOldNeighbors)
                    .doesNotHaveDuplicates();
            }

            var newNeighbors = allNewNeighbors.get(nodeId);
            if (newNeighbors != null) {
                var neighbors = StreamSupport.stream(newNeighbors.spliterator(), false)
                    .mapToLong(c -> c.value)
                    .toArray();
                assertThat(neighbors)
                    .hasSizeBetween(1, sampledK)
                    .doesNotContain(nodeId)
                    .containsAnyOf(possibleNewNeighbors)
                    .doesNotHaveDuplicates();
            }
        }
    }
}
