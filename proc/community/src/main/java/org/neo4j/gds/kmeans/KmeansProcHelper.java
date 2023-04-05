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
package org.neo4j.gds.kmeans;

import java.util.ArrayList;
import java.util.List;

final class KmeansProcHelper {
    private KmeansProcHelper() {}

    static List<List<Double>> arrayMatrixToListMatrix(double[][] matrix) {
        var result = new ArrayList<List<Double>>();

        for (double[] row : matrix) {
            List<Double> rowList = new ArrayList<>();
            result.add(rowList);
            for (double column : row)
                rowList.add(column);
        }
        return result;
    }
}
