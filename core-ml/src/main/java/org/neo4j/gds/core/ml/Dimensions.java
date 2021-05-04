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
package org.neo4j.gds.core.ml;

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

}
