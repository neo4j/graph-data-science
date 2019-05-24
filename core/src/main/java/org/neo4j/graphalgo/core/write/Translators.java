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

/**
 * @author mknblch
 */
public class Translators {

    public static final PropertyTranslator<AtomicDoubleArray> ATOMIC_DOUBLE_ARRAY_TRANSLATOR =
            (PropertyTranslator.OfDouble<AtomicDoubleArray>) (data, nodeId) -> data.get((int) nodeId);

    public static final PropertyTranslator<AtomicIntegerArray> ATOMIC_INTEGER_ARRAY_TRANSLATOR =
            (PropertyTranslator.OfInt<AtomicIntegerArray>) (data, nodeId) -> data.get((int) nodeId);

    public static final PropertyTranslator.OfDouble<double[]> DOUBLE_ARRAY_TRANSLATOR =
            (PropertyTranslator.OfDouble<double[]>) (data, nodeId) -> data[(int) nodeId];

    public static final PropertyTranslator.OfInt<int[]> INT_ARRAY_TRANSLATOR =
            (PropertyTranslator.OfInt<int[]>) (data, nodeId) -> data[(int) nodeId];

    public static final PropertyTranslator.OfDouble<IntDoubleMap> INT_DOUBLE_MAP_TRANSLATOR =
            (PropertyTranslator.OfDouble<IntDoubleMap>) (data, nodeId) -> data.get((int) nodeId);

    public static final PropertyTranslator.OfOptionalInt<int[]> OPTIONAL_INT_ARRAY_TRANSLATOR =
            (PropertyTranslator.OfOptionalInt<int[]>) (data, nodeId) -> data[(int) nodeId];

    public static final PropertyTranslator.OfOptionalDouble<IntDoubleMap> OPTIONAL_DOUBLE_MAP_TRANSLATOR =
            (PropertyTranslator.OfOptionalDouble<IntDoubleMap>) (data, nodeId) -> data.getOrDefault((int) nodeId, -1D);

}
