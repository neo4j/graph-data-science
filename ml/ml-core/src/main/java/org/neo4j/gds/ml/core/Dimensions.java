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
package org.neo4j.gds.ml.core;

import java.util.Arrays;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class Dimensions {

    public static final int ROWS_INDEX = 0;
    public static final int COLUMNS_INDEX = 1;

    private Dimensions() {}

    public static int[] scalar() {
        return new int[]{1};
    }

    public static int[] vector(int size) {
        return new int[]{size};
    }

    public static int[] matrix(int rows, int cols) {
        return new int[]{rows, cols};
    }

    public static boolean isVector(int[] dimensions) {
        // at most one dimensions can be larger than 1
        var length = dimensions.length;
        var dimLargerOne = 0;
        for (int i = 0; i < length; i++) {
            if (dimensions[i] > 1) {
                dimLargerOne++;
            }
        }

        return dimLargerOne <= 1;
    }

    public static boolean isScalar(int[] dimensions) {
        return totalSize(dimensions) == 1;
    }

    public static int totalSize(int[] dimensions) {
        if (dimensions.length == 0) {
            return 0;
        }
        int totalSize = 1;
        for (int dim : dimensions) {
            totalSize *= dim;
        }
        return totalSize;
    }

    public static String render(int[] dimensions) {
        if (dimensions.length == 0) {
            return "Scalar";
        } else if (dimensions.length == 1) {
            return "Vector(" + dimensions[0] + ")";
        } else if (dimensions.length == 2) {
            return "Matrix(" + dimensions[0] + ", " + dimensions[1]  + ")";
        }

        return formatWithLocale("%d-Dim Tensor: %s", dimensions.length, Arrays.toString(dimensions));
    }
}
