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
package org.neo4j.gds.ml.core.helper;

import org.neo4j.gds.ml.core.tensor.Tensor;

public final class TensorTestUtils {
    private TensorTestUtils() {}

    public static boolean containsNaN(Tensor<?> tensor) {
        for (int idx = 0; idx < tensor.totalSize(); idx++) {
            if (Double.isNaN(tensor.dataAt(idx))) {
                return true;
            }
        }
        return false;
    }

    public static boolean containsValidValues(Tensor<?> tensor) {
        return !containsInfinity(tensor)
               && !containsNaN(tensor);
    }

    private static boolean containsInfinity(Tensor<?> tensor) {
        for (int idx = 0; idx < tensor.totalSize(); idx++) {
            if (Double.isInfinite(tensor.dataAt(idx))) {
                return true;
            }
        }
        return false;
    }
}
