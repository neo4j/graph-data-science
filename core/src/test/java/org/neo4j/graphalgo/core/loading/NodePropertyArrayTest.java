/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.core.loading;

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;

import java.util.OptionalLong;
import java.util.concurrent.Executors;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class NodePropertyArrayTest {

    @Test
    void emptyProperties() {
        var properties = NodePropertiesBuilder.of(
            100_000,
            AllocationTracker.EMPTY,
            -1
        ).build();

        assertEquals(0L, properties.size());
        assertEquals(OptionalLong.empty(), properties.getMaxPropertyValue());
        assertEquals(42.0, properties.nodeProperty(0, 42.0));
    }

    @Test
    void returnsValuesThatHaveBeenSet() {
        var properties = NodePropertiesBuilder.of(2L, 42.0, b -> b.set(1, 1.0));

        assertEquals(1.0, properties.nodeProperty(1));
        assertEquals(1.0, properties.nodeProperty(1, 42.0));
    }

    @Test
    void returnsDefaultOnMissingEntries() {
        var expectedImplicitDefault = 42.0;
        var expectedExplicitDefault = 1337.0;

        var properties = NodePropertiesBuilder.of(2L, expectedImplicitDefault, b -> {});

        assertEquals(expectedImplicitDefault, properties.nodeProperty(2));
        assertEquals(expectedExplicitDefault, properties.nodeProperty(2, expectedExplicitDefault));
    }

    @Test
    void returnNaNIfItWasSet() {
        var properties = NodePropertiesBuilder.of(2L, 42.0, b -> b.set(1, Double.NaN));

        assertEquals(42.0, properties.nodeProperty(0));
        assertEquals(Double.NaN, properties.nodeProperty(1));
    }

    @Test
    void trackMaxValue() {
        var properties = NodePropertiesBuilder.of(2L, 0.0, b -> {
            b.set(0, 42.0);
            b.set(1, 21.0);
        });
        var maxPropertyValue = properties.getMaxPropertyValue();
        assertTrue(maxPropertyValue.isPresent());
        assertEquals(42, maxPropertyValue.getAsLong());
    }

    @Test
    void hasSize() {
        var properties = NodePropertiesBuilder.of(2L, 0.0, b -> {
            b.set(0, 42.0);
            b.set(1, 21.0);
        });
        assertEquals(2, properties.size());
    }

    @Test
    void threadSafety() throws InterruptedException {
        var pool = Executors.newFixedThreadPool(2);
        var nodeSize = 100_000;
        var builder = NodePropertiesBuilder.of(nodeSize, AllocationTracker.EMPTY, Double.NaN);

        var phaser = new Phaser(3);
        pool.execute(() -> {
            // wait for start signal
            phaser.arriveAndAwaitAdvance();
            // first task, set the value 2 on every other node, except for 1338 which is set to 2^41
            // the idea is that the maxValue set will read the currentMax of 2, decide to update to 2^41 and write
            // that value, while the other thread will write 2^42 in the meantime. If that happens,
            // this thread would overwrite a new maxValue.
            for (int i = 0; i < nodeSize; i += 2) {
                builder.set(i, i == 1338 ? 0x1p41 : 2.0);
            }
        });
        pool.execute(() -> {
            // wait for start signal
            phaser.arriveAndAwaitAdvance();
            // second task, sets the value 1 on every other node, except for 1337 which is set to 2^42
            // Depending on thread scheduling, the write for 2^42 might be overwritten by the first thread
            for (int i = 1; i < nodeSize; i += 2) {
                builder.set(i, i == 1337 ? 0x1p42 : 1.0);
            }
        });

        phaser.arriveAndAwaitAdvance();

        pool.shutdown();
        pool.awaitTermination(10, TimeUnit.SECONDS);

        var properties = builder.build();
        for (int i = 0; i < nodeSize; i++) {
            var expected = i == 1338 ? 0x1p41 : i == 1337 ? 0x1p42 : i % 2 == 0 ? 2.0 : 1.0;
            assertEquals(expected, properties.nodeProperty(i));
        }
        assertEquals(nodeSize, properties.size());
        var maxPropertyValue = properties.getMaxPropertyValue();
        assertTrue(maxPropertyValue.isPresent());

        // If write were correctly ordered, this is always true
        // If, however, the write to maxValue were to be non-atomic
        // e.g. `this.maxValue = Math.max(value, this.maxValue);`
        // this would occasionally be 2^41.
        assertEquals(1L << 42, maxPropertyValue.getAsLong());
    }
}
