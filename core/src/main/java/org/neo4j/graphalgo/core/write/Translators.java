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
package org.neo4j.graphalgo.core.write;

import com.carrotsearch.hppc.IntDoubleMap;
import org.neo4j.graphalgo.core.utils.AtomicDoubleArray;

import java.util.concurrent.atomic.AtomicIntegerArray;

public class Translators {

    public static final PropertyTranslator.OfDouble<AtomicDoubleArray> ATOMIC_DOUBLE_ARRAY_TRANSLATOR =
        (data, nodeId) -> data.get((int) nodeId);

    public static final PropertyTranslator.OfInt<AtomicIntegerArray> ATOMIC_INTEGER_ARRAY_TRANSLATOR =
        (data, nodeId) -> data.get((int) nodeId);

    public static final PropertyTranslator.OfDouble<double[]> DOUBLE_ARRAY_TRANSLATOR =
        (data, nodeId) -> data[(int) nodeId];

    public static final PropertyTranslator.OfInt<int[]> INT_ARRAY_TRANSLATOR =
        (data, nodeId) -> data[(int) nodeId];

    public static final PropertyTranslator.OfDouble<IntDoubleMap> INT_DOUBLE_MAP_TRANSLATOR =
        (data, nodeId) -> data.get((int) nodeId);

    public static final PropertyTranslator.OfOptionalInt<int[]> OPTIONAL_INT_ARRAY_TRANSLATOR =
        (data, nodeId) -> data[(int) nodeId];
}
