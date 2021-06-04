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
package org.neo4j.gds.ml.core.tensor;

import org.neo4j.gds.ml.core.Dimensions;
import org.neo4j.graphalgo.core.utils.ArrayUtil;

import java.util.Arrays;

import static org.neo4j.gds.ml.core.Dimensions.ROWS_INDEX;
import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

public class Vector extends Tensor<Vector> {

    public Vector(double... elements) {
        super(elements, Dimensions.vector(elements.length));

    }

    public Vector(int size) {
        this(new double[size]);
    }

    public static Vector fill(double v, int length) {
        return new Vector(ArrayUtil.fill(v, length));
    }

    @Override
    public Vector zeros() {
        return fill(0D, length());
    }

    @Override
    public Vector copy() {
        return new Vector(data.clone());
    }

    @Override
    public Vector add(Vector b) {
        if (length() != b.length()) {
            throw new ArithmeticException(formatWithLocale(
                "Vector lengths must be equal, got %d + %d lengths",
                length(),
                b.length()
            ));
        }
        Vector sum = zeros();
        for (int i = 0; i < length(); ++i) {
            sum.data[i] = data[i] + b.data[i];
        }
        return sum;
    }

    @Override
    public String shortDescription() {
        return formatWithLocale("Vector(%d)", length());
    }

    public int length() {
        return dimensions[ROWS_INDEX];
    }
}
