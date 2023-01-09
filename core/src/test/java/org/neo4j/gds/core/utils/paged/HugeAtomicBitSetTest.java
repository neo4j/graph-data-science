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
package org.neo4j.gds.core.utils.paged;

import com.carrotsearch.hppc.BitSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.mem.BitUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Phaser;
import java.util.function.LongConsumer;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

class HugeAtomicBitSetTest {

    @Test
    void testSize() {
        var bitSet = HugeAtomicBitSet.create(1337);
        assertThat(bitSet.size()).isEqualTo(1337);
    }

    @ParameterizedTest
    @CsvSource({"0,1336", "0,63", "70,140"})
    void setRange(int startIndex, int endIndex) {
        var bitSet = HugeAtomicBitSet.create(1337);
        bitSet.set(startIndex, endIndex);
        for (int i = 0; i < bitSet.size(); i++) {
            if (i >= startIndex && i < endIndex) {
                assertThat(bitSet.get(i)).as(formatWithLocale("index %d expected to be true", i)).isTrue();
            } else {
                assertThat(bitSet.get(i)).as(formatWithLocale("index %d expected to be false", i)).isFalse();
            }
        }
    }

    @Test
    void setRangeDoesNotSetOutsideOfRange() {
        var bitSet = HugeAtomicBitSet.create(1337);
        bitSet.set(0, 1337);

        // need to go through the inner bitset to check for values outside of size()
        var hppcBitSet = toHppcBitSet(bitSet);
        var max = BitUtil.align(1337, 64);
        for (int i = 0; i < max; i++) {
            if (i < 1337) {
                assertThat(hppcBitSet.get(i)).as(formatWithLocale("index %d expected to be true", i)).isTrue();
            } else {
                assertThat(hppcBitSet.get(i)).as(formatWithLocale("index %d expected to be false", i)).isFalse();
            }
        }
    }

    @Test
    void setRangeParallel() {
        var bitSet = HugeAtomicBitSet.create(128);
        var phaser = new Phaser(5);
        var pool = Executors.newCachedThreadPool();
        pool.submit(new SetTask(bitSet, phaser, 0, 16));
        pool.submit(new SetTask(bitSet, phaser, 16, 32));
        pool.submit(new SetTask(bitSet, phaser, 40, 80));
        pool.submit(new SetTask(bitSet, phaser, 100, 127));
        phaser.arriveAndAwaitAdvance();
        phaser.arriveAndAwaitAdvance();

        for (int i = 0; i < bitSet.size(); i++) {
            if (i >= 0 && i < 32) assertThat(bitSet.get(i)).isTrue();
            else if (i >= 40 && i < 80) assertThat(bitSet.get(i)).isTrue();
            else if (i >= 100 && i < 127) assertThat(bitSet.get(i)).isTrue();
            else assertThat(bitSet.get(i)).isFalse();
        }
    }

    @Test
    void setRangeParallelDoesNotSetOutsideOfRange() {
        var bitSet = HugeAtomicBitSet.create(168);
        var phaser = new Phaser(5);
        var pool = Executors.newCachedThreadPool();
        pool.submit(new SetTask(bitSet, phaser, 0, 42));
        pool.submit(new SetTask(bitSet, phaser, 42, 84));
        pool.submit(new SetTask(bitSet, phaser, 92, 122));
        pool.submit(new SetTask(bitSet, phaser, 133, 137));
        phaser.arriveAndAwaitAdvance();
        phaser.arriveAndAwaitAdvance();

        // need to go through the inner bitset to check for values outside of size()
        var hppcBitSet = toHppcBitSet(bitSet);
        var max = BitUtil.align(168, 64);
        for (int i = 0; i < max; i++) {
            if (i < 84 || (i >= 92 && i < 122) || (i >= 133 && i < 137)) {
                assertThat(hppcBitSet.get(i)).as(formatWithLocale("index %d expected to be true", i)).isTrue();
            } else {
                assertThat(hppcBitSet.get(i)).as(formatWithLocale("index %d expected to be false", i)).isFalse();
            }
        }
    }

    private static final class SetTask implements Runnable {
        private final HugeAtomicBitSet habs;
        private final Phaser phaser;
        private final long startIndex;
        private final long endIndex;

        private SetTask(HugeAtomicBitSet habs, Phaser phaser, long startIndex, long endIndex) {
            this.habs = habs;
            this.phaser = phaser;
            this.startIndex = startIndex;
            this.endIndex = endIndex;
        }

        @Override
        public void run() {
            phaser.arriveAndAwaitAdvance();
            habs.set(startIndex, endIndex);
            phaser.arrive();
        }
    }

    @Test
    void testFlipping() {
        var bitSet = HugeAtomicBitSet.create(42);
        bitSet.flip(41);
        assertThat(bitSet.get(41)).isTrue();
        bitSet.flip(41);
        assertThat(bitSet.get(41)).isFalse();
    }

    @Test
    void testCardinality() {
        var bitSet = HugeAtomicBitSet.create(42);
        assertThat(bitSet.cardinality()).isEqualTo(0L);

        bitSet.set(41);
        assertThat(bitSet.cardinality()).isEqualTo(1L);

        for (long i = 0; i < bitSet.size(); i++) {
            bitSet.set(i);
        }
        assertThat(bitSet.cardinality()).isEqualTo(42L);
    }

    @Test
    void testClearAll() {
        var bitSet = HugeAtomicBitSet.create(100);
        for (long i = 0; i < bitSet.size(); i++) {
            bitSet.set(i);
        }

        bitSet.clear();

        for (long i = 0; i < bitSet.size(); i++) {
            assertThat(bitSet.get(i)).isFalse();
        }
    }

    @Test
    void testIsEmpty() {
        var atomicBitSet = HugeAtomicBitSet.create(42);
        assertThat(atomicBitSet.isEmpty()).isTrue();
        atomicBitSet.set(23);
        assertThat(atomicBitSet.isEmpty()).isFalse();
        atomicBitSet.flip(23);
        assertThat(atomicBitSet.isEmpty()).isTrue();
    }

    @Test
    void testAllSet() {
        var atomicBitSet = HugeAtomicBitSet.create(42);
        assertThat(atomicBitSet.allSet()).isFalse();
        atomicBitSet.set(23);
        assertThat(atomicBitSet.allSet()).isFalse();
        atomicBitSet.set(0, 42);
        assertThat(atomicBitSet.allSet()).isTrue();
        atomicBitSet.flip(23);
        assertThat(atomicBitSet.allSet()).isFalse();
    }

    private static BitSet toHppcBitSet(HugeAtomicBitSet habs) {
        var bitSet = new BitSet(habs.size());
        habs.forEachSetBit(bitSet::set);
        return bitSet;
    }

    @ParameterizedTest
    @MethodSource("suppliers")
    void testGetSetClear(HabsSupplier provider) {
        var bitSet = provider.get(42);
        assertThat(bitSet.get(7)).isFalse();
        assertThat(bitSet.get(8)).isFalse();
        assertThat(bitSet.get(9)).isFalse();
        bitSet.set(8);
        assertThat(bitSet.get(7)).isFalse();
        assertThat(bitSet.get(8)).isTrue();
        assertThat(bitSet.get(9)).isFalse();
        bitSet.clear(8);
        assertThat(bitSet.get(7)).isFalse();
        assertThat(bitSet.get(8)).isFalse();
        assertThat(bitSet.get(9)).isFalse();
    }

    @ParameterizedTest
    @MethodSource("suppliers")
    void testForEachSetBit(HabsSupplier supplier) {
        var bitSet = supplier.get(4096);

        var expected = List.of(0L, 1L, 3L, 7L, 15L, 63L, 64L, 72L, 128L, 420L, 1337L, 4095L);
        expected.forEach(bitSet::set);

        var actual = new ArrayList<Long>();
        bitSet.forEachSetBit(actual::add);

        assertThat(actual).isEqualTo(expected);
    }

    @ParameterizedTest
    @MethodSource("suppliers")
    void getAndSetReturnsTrueIfTheBitWasSet(HabsSupplier supplier) {
        var bitSet = supplier.get(1);
        bitSet.set(0);
        assertThat(bitSet.getAndSet(0)).isTrue();
    }

    @ParameterizedTest
    @MethodSource("suppliers")
    void getAndSetReturnsFalseIfTheBitWasNotSet(HabsSupplier supplier) {
        var bitSet = supplier.get(1);
        assertThat(bitSet.getAndSet(0)).isFalse();
    }

    @ParameterizedTest
    @MethodSource("suppliers")
    void getAndSetSetsTheBit(HabsSupplier supplier) {
        var bitSet = supplier.get(1);
        assertThat(bitSet.get(0)).isFalse();
        bitSet.getAndSet(0);
        assertThat(bitSet.get(0)).isTrue();
    }

    /**
     * Represents shared methods from
     * {@link org.neo4j.gds.core.utils.paged.HugeAtomicBitSet} and
     * {@link HugeAtomicGrowingBitSet}.
     */
    interface CommonHabsOps {
        boolean get(long index);

        void set(long index);

        boolean getAndSet(long index);

        void forEachSetBit(LongConsumer consumer);

        long cardinality();

        void clear(long index);
    }

    interface HabsSupplier {
        CommonHabsOps get(long size);
    }

    static Stream<HabsSupplier> suppliers() {
        return Stream.of(
            (size) -> {
                var habs = HugeAtomicBitSet.create(size);
                return new CommonHabsOps() {
                    @Override
                    public boolean get(long index) {
                        return habs.get(index);
                    }

                    @Override
                    public void set(long index) {
                        habs.set(index);
                    }

                    @Override
                    public boolean getAndSet(long index) {
                        return habs.getAndSet(index);
                    }

                    @Override
                    public void forEachSetBit(LongConsumer consumer) {
                        habs.forEachSetBit(consumer);
                    }

                    @Override
                    public long cardinality() {
                        return habs.cardinality();
                    }

                    @Override
                    public void clear(long index) {
                        habs.clear(index);
                    }
                };
            },
            (size) -> {
                var habs = HugeAtomicGrowingBitSet.create(size);
                return new CommonHabsOps() {
                    @Override
                    public boolean get(long index) {
                        return habs.get(index);
                    }

                    @Override
                    public void set(long index) {
                        habs.set(index);
                    }

                    @Override
                    public boolean getAndSet(long index) {
                        return habs.getAndSet(index);
                    }

                    @Override
                    public void forEachSetBit(LongConsumer consumer) {
                        habs.forEachSetBit(consumer);
                    }

                    @Override
                    public long cardinality() {
                        return habs.cardinality();
                    }

                    @Override
                    public void clear(long index) {
                        habs.clear(index);
                    }
                };
            }
        );
    }
}
