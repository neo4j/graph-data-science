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

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.function.DoubleUnaryOperator;

/**
 * A {@code double} value that may be updated atomically.
 *
 * See the {@link VarHandle} specification for descriptions
 * of the properties of atomic accesses.
 *
 * The implementation is based on a {@link VarHandle} guarding
 * the access to a primitive {@code double} field. For double,
 * numeric (e.g. {@code getAndAdd}) as well as bitwise
 * (e.g. {@code getOpaque}) atomic update method access modes
 * compare values using their bitwise representation
 * (see {@link Double#doubleToRawLongBits(double)}).
 */
public final class AtomicDouble extends Number {

    private static final VarHandle VALUE;

    @SuppressWarnings("unused")
    private volatile double value;

    static {
        try {
            VALUE = MethodHandles.lookup().findVarHandle(AtomicDouble.class, "value", double.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    /**
     * Creates a new AtomicDouble with initial value {@code 0}.
     */
    public AtomicDouble() {
    }

    /**
     * Creates a new AtomicDouble with the given initial value.
     *
     * @param initialValue the initial value
     */
    public AtomicDouble(double initialValue) {
        VALUE.set(this, initialValue);
    }

    /**
     * Returns the current value,
     * with memory effects as specified by {@link VarHandle#getVolatile}.
     *
     * @return the current value
     */
    public double get() {
        return (double) VALUE.getVolatile(this);
    }

    /**
     * Sets the value to {@code newValue},
     * with memory effects as specified by {@link VarHandle#setVolatile}.
     *
     * @param newValue the new value
     */
    public void set(double newValue) {
        VALUE.setVolatile(this, newValue);
    }

    /**
     * Sets the value to {@code newValue}, with memory effects as specified
     * by {@link VarHandle#setRelease}.
     *
     * @param newValue the new value
     */
    public void lazySet(long newValue) {
        VALUE.setRelease(this, newValue);
    }

    /**
     * Atomically sets the value to {@code newValue} and returns the old value,
     * with memory effects as specified by {@link VarHandle#getAndSet}.
     *
     * @param newValue the new value
     * @return the previous value
     */
    public double getAndSet(double newValue) {
        return (double) VALUE.getAndSet(this, newValue);
    }

    /**
     * Atomically adds the given value to the current value,
     * with memory effects as specified by {@link VarHandle#getAndAdd}.
     *
     * @param delta the value to add
     * @return the previous value
     */
    public double getAndAdd(double delta) {
        return (double) VALUE.getAndAdd(this, delta);
    }

    /**
     * Atomically adds the given value to the current value,
     * with memory effects as specified by {@link VarHandle#getAndAdd}.
     *
     * @param delta the value to add
     * @return the updated value
     */
    public double addAndGet(double delta) {
        return (double) VALUE.getAndAdd(this, delta) + delta;
    }

    /**
     * Atomically updates (with memory effects as specified by {@link
     * VarHandle#compareAndSet}) the current value with the results of
     * applying the given function, returning the previous value. The
     * function should be side-effect-free, since it may be re-applied
     * when attempted updates fail due to contention among threads.
     *
     * @param updateFunction a side-effect-free function
     * @return the previous value
     */
    public double getAndUpdate(DoubleUnaryOperator updateFunction) {
        while (true) {
            double current = get();
            double next = updateFunction.applyAsDouble(current);

            if (weakCompareAndSetVolatile(current, next)) {
                return current;
            }
        }
    }

    /**
     * Atomically updates the current value with the results of applying the given function.
     *
     * @param updateFunction the update function
     * @return the updated value
     */
    public double updateAndGet(DoubleUnaryOperator updateFunction) {
        while (true) {
            double current = get();
            double next = updateFunction.applyAsDouble(current);

            if (weakCompareAndSetVolatile(current, next)) {
                return next;
            }
        }
    }

    /**
     * Returns the current value of this {@code AtomicDouble} as an {@code int}
     * after a narrowing primitive conversion, with memory effects as specified
     * by {@link VarHandle#getVolatile}.
     *
     * @return the current value
     */
    @Override
    public int intValue() {
        return (int) get();
    }

    /**
     * Returns the current value of this {@code AtomicDouble} as an {@code long}
     * after a narrowing primitive conversion, with memory effects as specified
     * by {@link VarHandle#getVolatile}.
     *
     * @return the current value
     */
    @Override
    public long longValue() {
        return (long) get();
    }

    /**
     * Returns the current value of this {@code AtomicDouble} as an {@code float}
     * after a narrowing primitive conversion, with memory effects as specified
     * by {@link VarHandle#getVolatile}.
     *
     * @return the current value
     */
    @Override
    public float floatValue() {
        return (float) get();
    }

    /**
     * Returns the current value of this {@code AtomicDouble} as an {@code long}
     * with memory effects as specified by {@link VarHandle#getVolatile}.
     * Equivalent to {@link #get()}.
     *
     * @return the current value
     */
    @Override
    public double doubleValue() {
        return get();
    }

    /**
     * Returns the current value, with memory semantics of reading as if the
     * variable was declared non-{@code volatile}.
     *
     * @return the current value
     */
    public double getPlain() {
        return (double) VALUE.get(this);
    }

    /**
     * Sets the value to {@code newValue}, with memory semantics
     * of setting as if the variable was declared non-{@code volatile}
     * and non-{@code final}.
     *
     * @param newValue the new value
     */
    public void setPlain(double newValue) {
        VALUE.set(this, newValue);
    }

    /**
     * Returns the current value, with memory effects as specified
     * by {@link VarHandle#getOpaque}.
     *
     * @return the current value
     */
    public double getOpaque() {
        return (double) VALUE.getOpaque(this);
    }

    /**
     * Sets the value to {@code newValue}, with memory effects as specified
     * by {@link VarHandle#setOpaque}.
     *
     * @param newValue the new value
     */
    public void setOpaque(double newValue) {
        VALUE.setOpaque(this, newValue);
    }

    /**
     * Returns the current value, with memory effects as specified
     * by {@link VarHandle#getAcquire}.
     *
     * @return the current value
     */
    public double getAcquire() {
        return (double) VALUE.getAcquire(this);
    }

    /**
     * Sets the value to {@code newValue}, with memory effects as specified
     * by {@link VarHandle#setRelease}.
     *
     * @param newValue the new value
     */
    public void setRelease(double newValue) {
        VALUE.setRelease(this, newValue);
    }

    /**
     * Atomically sets the value to {@code newValue}
     * if the current value {@code == expectedValue},
     * with memory effects as specified by {@link VarHandle#compareAndSet}.
     *
     * @param expectedValue the expected value
     * @param newValue      the new value
     * @return {@code true} if successful. False return indicates that
     *     the actual value was not equal to the expected value.
     */
    public boolean compareAndSet(double expectedValue, double newValue) {
        return VALUE.compareAndSet(this, expectedValue, newValue);
    }

    /**
     * Atomically sets the value to {@code newValue} if the current value,
     * referred to as the <em>witness value</em>, {@code == expectedValue},
     * with memory effects as specified by
     * {@link VarHandle#compareAndExchange}.
     *
     * @param expectedValue the expected value
     * @param newValue      the new value
     * @return the witness value, which will be the same as the expected value if successful
     */
    public double compareAndExchange(double expectedValue, double newValue) {
        return (double) VALUE.compareAndExchange(this, expectedValue, newValue);
    }

    /**
     * Atomically sets the value to {@code newValue} if the current value,
     * referred to as the <em>witness value</em>, {@code == expectedValue},
     * with memory effects as specified by {@link VarHandle#compareAndExchangeAcquire}.
     *
     * @param expectedValue the expected value
     * @param newValue      the new value
     * @return the witness value, which will be the same as the expected value if successful
     */
    public double compareAndExchangeAcquire(double expectedValue, double newValue) {
        return (double) VALUE.compareAndExchangeAcquire(this, expectedValue, newValue);
    }

    /**
     * Atomically sets the value to {@code newValue} if the current value,
     * referred to as the <em>witness value</em>, {@code == expectedValue},
     * with memory effects as specified by
     * {@link VarHandle#compareAndExchangeRelease}.
     *
     * @param expectedValue the expected value
     * @param newValue      the new value
     * @return the witness value, which will be the same as the
     *     expected value if successful
     */
    public double compareAndExchangeRelease(double expectedValue, double newValue) {
        return (double) VALUE.compareAndExchangeRelease(this, expectedValue, newValue);
    }

    /**
     * Possibly atomically sets the value to {@code newValue}
     * if the current value {@code == expectedValue},
     * with memory effects as specified by {@link VarHandle#weakCompareAndSetPlain}.
     *
     * @param expectedValue the expected value
     * @param newValue      the new value
     * @return {@code true} if successful
     */
    public boolean weakCompareAndSetPlain(double expectedValue, double newValue) {
        return VALUE.weakCompareAndSetPlain(this, expectedValue, newValue);
    }

    /**
     * Possibly atomically sets the value to {@code newValue}
     * if the current value {@code == expectedValue},
     * with memory effects as specified by
     * {@link VarHandle#weakCompareAndSet}.
     *
     * @param expectedValue the expected value
     * @param newValue      the new value
     * @return {@code true} if successful
     */
    public boolean weakCompareAndSetVolatile(double expectedValue, double newValue) {
        return VALUE.weakCompareAndSet(this, expectedValue, newValue);
    }

    /**
     * Possibly atomically sets the value to {@code newValue}
     * if the current value {@code == expectedValue},
     * with memory effects as specified by
     * {@link VarHandle#weakCompareAndSetAcquire}.
     *
     * @param expectedValue the expected value
     * @param newValue      the new value
     * @return {@code true} if successful
     */
    public boolean weakCompareAndSetAcquire(double expectedValue, double newValue) {
        return VALUE.weakCompareAndSetAcquire(this, expectedValue, newValue);
    }

    /**
     * Possibly atomically sets the value to {@code newValue}
     * if the current value {@code == expectedValue},
     * with memory effects as specified by
     * {@link VarHandle#weakCompareAndSetRelease}.
     *
     * @param expectedValue the expected value
     * @param newValue      the new value
     * @return {@code true} if successful
     */
    public boolean weakCompareAndSetRelease(double expectedValue, double newValue) {
        return VALUE.weakCompareAndSetRelease(this, expectedValue, newValue);
    }

    /**
     * Returns the String representation of the current value.
     *
     * @return the String representation of the current value
     */
    @Override
    public String toString() {
        return Double.toString(get());
    }
}
