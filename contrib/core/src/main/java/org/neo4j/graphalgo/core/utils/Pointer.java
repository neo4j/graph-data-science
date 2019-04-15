/*
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
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

import java.util.function.DoubleToLongFunction;
import java.util.function.Function;

/**
 * @author mknblch
 */
public class Pointer {

    public static class BoolPointer {
        public boolean v;

        public BoolPointer(boolean v) {
            this.v = v;
        }

        public BoolPointer map(Function<Boolean, Boolean> function) {
            v = function.apply(v);
            return this;
        }
    }

    public static class IntPointer {
        public int v;

        public IntPointer(int v) {
            this.v = v;
        }

        public IntPointer map(Function<Integer, Integer> function) {
            v = function.apply(v);
            return this;
        }
    }

    public static class LongPointer {
        public long v;

        public LongPointer(long v) {
            this.v = v;
        }

        public LongPointer map(Function<Long, Long> function) {
            v = function.apply(v);
            return this;
        }
    }

    public static class DoublePointer {
        public double v;

        public DoublePointer(double v) {
            this.v = v;
        }

        public DoublePointer map(DoubleToDoubleFunction function) {
            v = function.apply(v);
            return this;
        }
    }

    @FunctionalInterface
    public interface DoubleToDoubleFunction {
        double apply(double var1);
    }

    public static class GenericPointer<G> {
        public G v;

        public GenericPointer(G v) {
            this.v = v;
        }

        public GenericPointer<G> map(Function<G, G> function) {
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

    public static <T> GenericPointer wrap(T v) {
        return new GenericPointer<>(v);
    }
}
