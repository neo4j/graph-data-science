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
package org.neo4j.gds.core.concurrency;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.utils.ExceptionUtil;

import java.util.concurrent.CountDownLatch;

import static org.assertj.core.api.Assertions.assertThat;

class AtomicDoubleTest {

    @Property
    void get(@ForAll double v) {
        var value = new AtomicDouble(v);

        assertThat(value.get()).isEqualTo(v);
        assertThat(value.getPlain()).isEqualTo(v);
        assertThat(value.getOpaque()).isEqualTo(v);
        assertThat(value.getAcquire()).isEqualTo(v);
    }

    @Property
    void set(@ForAll double v) {
        var value = new AtomicDouble(0);

        value.set(v);
        assertThat(value.get()).isEqualTo(v);
        value.set(0);

        value.setOpaque(v);
        assertThat(value.get()).isEqualTo(v);
        value.set(0);

        value.setPlain(v);
        assertThat(value.get()).isEqualTo(v);
        value.set(0);

        value.setRelease(v);
        assertThat(value.get()).isEqualTo(v);
    }

    @Property
    void intValue(@ForAll double v) {
        var value = new AtomicDouble(v);

        assertThat(value.intValue()).isEqualTo((int) v);
    }

    @Property
    void longValue(@ForAll double v) {
        var value = new AtomicDouble(v);

        assertThat(value.longValue()).isEqualTo((long) v);
    }

    @Property
    void floatValue(@ForAll double v) {
        var value = new AtomicDouble(v);

        assertThat(value.floatValue()).isEqualTo((float) v);
    }

    @Property
    void doubleValue(@ForAll double v) {
        var value = new AtomicDouble(v);

        assertThat(value.doubleValue()).isEqualTo(v);
    }

    @Property
    void compareAndSetPositive(@ForAll double v0, @ForAll double v1) {
        var atomicValue = new AtomicDouble(v0);
        var witness = atomicValue.compareAndSet(v0, v1);

        assertThat(witness).isTrue();
        assertThat(atomicValue.get()).isEqualTo(v1);
    }

    @Property
    void compareAndSetNegative(@ForAll double v0, @ForAll double v1) {
        var atomicValue = new AtomicDouble(v0);
        var v0Neg = Double.longBitsToDouble(~Double.doubleToLongBits(v0));
        var witness = atomicValue.compareAndSet(v0Neg, v1);

        assertThat(witness).isFalse();
        assertThat(atomicValue.get()).isEqualTo(v0);
    }

    @Test
    void compareAndSetParallel() {
        var concurrency = 4;
        var value = new AtomicDouble();

        execute(concurrency, () -> {
            for (int j = 0; j < 42; j++) {
                var current = value.get();
                while (!value.compareAndSet(current, current + j)) {
                    current = value.get();
                }
            }
        });

        assertThat(value.get()).isEqualTo(concurrency * 42 * 41 / 2.0);
    }

    @Property
    void weakCompareAndSetPlainPositive(@ForAll double v0, @ForAll double v1) {
        var atomicValue = new AtomicDouble(v0);
        var witness = atomicValue.weakCompareAndSetPlain(v0, v1);

        assertThat(witness).isTrue();
        assertThat(atomicValue.get()).isEqualTo(v1);
    }

    @Property
    void weakCompareAndSetPlainNegative(@ForAll double v0, @ForAll double v1) {
        var atomicValue = new AtomicDouble(v0);
        var v0Neg = Double.longBitsToDouble(~Double.doubleToLongBits(v0));
        var witness = atomicValue.weakCompareAndSetPlain(v0Neg, v1);

        assertThat(witness).isFalse();
        assertThat(atomicValue.get()).isEqualTo(v0);
    }

    @Property
    void weakCompareAndSetVolatilePositive(@ForAll double v0, @ForAll double v1) {
        var atomicValue = new AtomicDouble(v0);
        var witness = atomicValue.weakCompareAndSetVolatile(v0, v1);

        assertThat(witness).isTrue();
        assertThat(atomicValue.get()).isEqualTo(v1);
    }

    @Property
    void weakCompareAndSetVolatileNegative(@ForAll double v0, @ForAll double v1) {
        var atomicValue = new AtomicDouble(v0);
        var v0Neg = Double.longBitsToDouble(~Double.doubleToLongBits(v0));
        var witness = atomicValue.weakCompareAndSetVolatile(v0Neg, v1);

        assertThat(witness).isFalse();
        assertThat(atomicValue.get()).isEqualTo(v0);
    }

    @Property
    void weakCompareAndSetAcquirePositive(@ForAll double v0, @ForAll double v1) {
        var atomicValue = new AtomicDouble(v0);
        var witness = atomicValue.weakCompareAndSetAcquire(v0, v1);

        assertThat(witness).isTrue();
        assertThat(atomicValue.get()).isEqualTo(v1);
    }

    @Property
    void weakCompareAndSetAcquireNegative(@ForAll double v0, @ForAll double v1) {
        var atomicValue = new AtomicDouble(v0);
        var v0Neg = Double.longBitsToDouble(~Double.doubleToLongBits(v0));
        var witness = atomicValue.weakCompareAndSetAcquire(v0Neg, v1);

        assertThat(witness).isFalse();
        assertThat(atomicValue.get()).isEqualTo(v0);
    }

    @Property
    void weakCompareAndSetReleasePositive(@ForAll double v0, @ForAll double v1) {
        var atomicValue = new AtomicDouble(v0);
        var witness = atomicValue.weakCompareAndSetRelease(v0, v1);

        assertThat(witness).isTrue();
        assertThat(atomicValue.get()).isEqualTo(v1);
    }

    @Property
    void weakCompareAndSetReleaseNegative(@ForAll double v0, @ForAll double v1) {
        var atomicValue = new AtomicDouble(v0);
        var v0Neg = Double.longBitsToDouble(~Double.doubleToLongBits(v0));
        var witness = atomicValue.weakCompareAndSetRelease(v0Neg, v1);

        assertThat(witness).isFalse();
        assertThat(atomicValue.get()).isEqualTo(v0);
    }

    @Property
    void compareAndExchangePositive(@ForAll double v0, @ForAll double v1) {
        var atomicValue = new AtomicDouble(v0);
        var witness = atomicValue.compareAndExchange(v0, v1);

        assertThat(v0).isEqualTo(witness);
        assertThat(atomicValue.get()).isEqualTo(v1);
    }

    @Property
    void compareAndExchangeNegative(@ForAll double v0, @ForAll double v1) {
        var atomicValue = new AtomicDouble(v0);
        var v0Neg = Double.longBitsToDouble(~Double.doubleToLongBits(v0));
        var witness = atomicValue.compareAndExchange(v0Neg, v1);

        assertThat(v0).isEqualTo(witness);
        assertThat(atomicValue.get()).isEqualTo(v0);
    }

    @Test
    void compareAndExchangeParallel() {
        var concurrency = 4;
        var value = new AtomicDouble();

        execute(concurrency, () -> {
            for (int j = 0; j < 42; j++) {
                var current = value.get();

                while (true) {
                    var next = current + j;
                    var witness = value.compareAndExchange(current, next);

                    if (Double.compare(witness, current) == 0) {
                        break;
                    }

                    current = witness;
                }
            }
        });

        assertThat(value.get()).isEqualTo(concurrency * 42 * 41 / 2.0);
    }

    @Property
    void compareAndExchangeAcquirePositive(@ForAll double v0, @ForAll double v1) {
        var atomicValue = new AtomicDouble(v0);
        var witness = atomicValue.compareAndExchangeAcquire(v0, v1);

        assertThat(v0).isEqualTo(witness);
        assertThat(atomicValue.get()).isEqualTo(v1);
    }

    @Property
    void compareAndExchangeAcquireNegative(@ForAll double v0, @ForAll double v1) {
        var atomicValue = new AtomicDouble(v0);
        var v0Neg = Double.longBitsToDouble(~Double.doubleToLongBits(v0));
        var witness = atomicValue.compareAndExchangeAcquire(v0Neg, v1);

        assertThat(v0).isEqualTo(witness);
        assertThat(atomicValue.get()).isEqualTo(v0);
    }

    @Property
    void compareAndExchangeReleasePositive(@ForAll double v0, @ForAll double v1) {
        var atomicValue = new AtomicDouble(v0);
        var witness = atomicValue.compareAndExchangeRelease(v0, v1);

        assertThat(v0).isEqualTo(witness);
        assertThat(atomicValue.get()).isEqualTo(v1);
    }

    @Property
    void compareAndExchangeReleaseNegative(@ForAll double v0, @ForAll double v1) {
        var atomicValue = new AtomicDouble(v0);
        var v0Neg = Double.longBitsToDouble(~Double.doubleToLongBits(v0));
        var witness = atomicValue.compareAndExchangeRelease(v0Neg, v1);

        assertThat(v0).isEqualTo(witness);
        assertThat(atomicValue.get()).isEqualTo(v0);
    }

    @Property
    void getAndAdd(@ForAll double v) {
        var value = new AtomicDouble(v);

        assertThat(value.getAndAdd(v)).isEqualTo(v);
        assertThat(value.get()).isEqualTo(2 * v);
    }

    @Test
    void getAndAddParallel() {
        var concurrency = 4;
        var value = new AtomicDouble();

        execute(concurrency, () -> {
            for (int j = 0; j < 42; j++) {
                value.getAndAdd(j);
            }
        });

        assertThat(value.get()).isEqualTo(concurrency * 42 * 41 / 2.0);
    }

    @Property
    void addAndGet(@ForAll double v) {
        var value = new AtomicDouble(v);

        assertThat(value.addAndGet(v)).isEqualTo(2 * v);
        assertThat(value.get()).isEqualTo(2 * v);
    }

    @Test
    void addAndGetParallel() {
        var concurrency = 4;
        var value = new AtomicDouble();

        execute(concurrency, () -> {
            for (int j = 0; j < 42; j++) {
                value.addAndGet(j);
            }
        });

        assertThat(value.get()).isEqualTo(concurrency * 42 * 41 / 2.0);
    }

    @Property
    void getAndUpdate(@ForAll double v) {
        var value = new AtomicDouble(v);

        assertThat(value.getAndUpdate(c -> c + v)).isEqualTo(v);
        assertThat(value.get()).isEqualTo(2 * v);
    }

    @Test
    void getAndUpdateParallel() {
        var concurrency = 4;
        var value = new AtomicDouble();

        execute(concurrency, () -> {
            for (int j = 0; j < 42; j++) {
                int k = j;
                value.getAndUpdate(v -> v + k);
            }
        });

        assertThat(value.get()).isEqualTo(concurrency * 42 * 41 / 2.0);
    }

    @Property
    void updateAndGet(@ForAll double v) {
        var value = new AtomicDouble(v);

        assertThat(value.updateAndGet(c -> c + v)).isEqualTo(2 * v);
        assertThat(value.get()).isEqualTo(2 * v);
    }

    @Test
    void updateAndGetParallel() {
        var concurrency = 4;
        var value = new AtomicDouble();

        execute(concurrency, () -> {
            for (int j = 0; j < 42; j++) {
                int k = j;
                value.updateAndGet(v -> v + k);
            }
        });

        assertThat(value.get()).isEqualTo(concurrency * 42 * 41 / 2.0);
    }

    private void execute(int concurrency, Runnable task) {
        var startLatch = new CountDownLatch(1);
        var stopLatch = new CountDownLatch(concurrency);

        for (int i = 0; i < concurrency; i++) {
            new Thread(() -> {
                ExceptionUtil.run(startLatch::await);
                task.run();
                stopLatch.countDown();
            }).start();
        }

        // unblock all threads
        startLatch.countDown();
        // block until all threads are finished
        ExceptionUtil.run(stopLatch::await);
    }
}
