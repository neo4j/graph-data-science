/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
package org.neo4j.graphalgo.core.utils;

import java.util.function.DoubleUnaryOperator;
import java.util.function.IntUnaryOperator;
import java.util.function.LongUnaryOperator;
import java.util.function.UnaryOperator;

public final class Pointer {

    private Pointer() {}

    public static class BoolPointer {
        public boolean v;

        public BoolPointer(boolean v) {
            this.v = v;
        }

        public BoolPointer map(BooleanUnaryOperator function) {
            v = function.applyAsBoolean(v);
            return this;
        }
    }

    @FunctionalInterface
    public interface BooleanUnaryOperator {
        boolean applyAsBoolean(boolean var1);
    }

    public static class IntPointer {
        public int v;

        public IntPointer(int v) {
            this.v = v;
        }

        public IntPointer map(IntUnaryOperator function) {
            v = function.applyAsInt(v);
            return this;
        }
    }

    public static class LongPointer {
        public long v;

        public LongPointer(long v) {
            this.v = v;
        }

        public LongPointer map(LongUnaryOperator function) {
            v = function.applyAsLong(v);
            return this;
        }
    }

    public static class DoublePointer {
        public double v;

        public DoublePointer(double v) {
            this.v = v;
        }

        public DoublePointer map(DoubleUnaryOperator function) {
            v = function.applyAsDouble(v);
            return this;
        }
    }

    public static class GenericPointer<G> {
        public G v;

        public GenericPointer(G v) {
            this.v = v;
        }

        public GenericPointer<G> map(UnaryOperator<G> function) {
            v = function.apply(v);
            return this;
        }
    }

    public static BoolPointer wrap(boolean v) {
        return new BoolPointer(v);
    }

    public static IntPointer wrap(int v) {
        return new IntPointer(v);
    }

    public static LongPointer wrap(long v) {
        return new LongPointer(v);
    }

    public static DoublePointer wrap(double v) {
        return new DoublePointer(v);
    }

    public static <T> GenericPointer<T> wrap(T v) {
        return new GenericPointer<>(v);
    }
}
