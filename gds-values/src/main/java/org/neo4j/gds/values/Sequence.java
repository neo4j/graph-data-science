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
package org.neo4j.gds.values;

/**
 * A value holding more than one element, as opposed to the scalar {@link IntegralValue} and
 * {@link FloatingPointValue}. It says nothing about how many elements there are, because its subtypes name
 * that differently: {@link Array} calls it {@code length()} and {@link Vector} calls it {@code dimension()}.
 *
 * <p>The overloads below are the dispatch table for {@link Object#equals(Object)}: an implementation that
 * does not know the other value's backing type hands its own array to the other value, which resolves the
 * comparison from the overload that matches. They cover the primitive number types only, since those are
 * the only elements a sequence currently holds; a non-numeric element type would add its own overload.
 */
public interface Sequence extends GdsValue {
    boolean equals(byte[] other);
    boolean equals(short[] other);
    boolean equals(int[] other);
    boolean equals(long[] other);
    boolean equals(float[] other);
    boolean equals(double[] other);
}
