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
package org.neo4j.gds.core.ml.tensor;

import static org.neo4j.gds.core.ml.Dimensions.COLUMNS_INDEX;
import static org.neo4j.gds.core.ml.Dimensions.ROWS_INDEX;
import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

public final class TensorFactory {

    private TensorFactory() {}

    public static Tensor<?> constant(double v, int[] dimensions) {
        if (dimensions.length == 1 && dimensions[ROWS_INDEX] == 1) {
            return new Scalar(v);
        } else if (dimensions.length == 1 && dimensions[ROWS_INDEX] > 1) {
            return Vector.fill(v, dimensions[ROWS_INDEX]);
            // TODO: sort out if a (1, 2) is a matrix or a vector vs (2, 1) vector or matrix?
        } else if (dimensions.length == 2 && dimensions[ROWS_INDEX] > 0 && dimensions[COLUMNS_INDEX] > 0) {
            return Matrix.fill(v, dimensions[ROWS_INDEX], dimensions[COLUMNS_INDEX]);
        } else {
            throw new IllegalArgumentException(formatWithLocale(
                "Tensor of dimensions greater than 2 are not supported, got %d dimensions",
                dimensions.length
            ));
        }
    }
}
