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
package org.neo4j.gds.api;

import static org.neo4j.gds.api.AdjacencyCursor.NOT_FOUND;

public final class AdjacencyCursorUtils {

    private AdjacencyCursorUtils() {}

    public static <CURSOR extends AdjacencyCursor> long advance(CURSOR adjacencyList, long start, long target) {
        long current = start;
        while (current != NOT_FOUND && current < target) {
            current = next(adjacencyList);
        }
        return current;
    }

    public static <CURSOR extends AdjacencyCursor> long advance(CURSOR adjacencyList, long target) {
        long current = peek(adjacencyList);
        while (current != NOT_FOUND && current < target) {
            current = next(adjacencyList);
        }
        return current;
    }


    public static <CURSOR extends AdjacencyCursor> long next(CURSOR adjacencyList) {

        if (!adjacencyList.hasNextVLong()) {
            return NOT_FOUND;
        }
        var value = adjacencyList.nextVLong();

        while (peek(adjacencyList) == value) {
            adjacencyList.nextVLong();
        }

        return value;
    }

    public static <CURSOR extends AdjacencyCursor> long peek(CURSOR adjacencyList) {

        if (!adjacencyList.hasNextVLong()) {
            return NOT_FOUND;
        }
        return adjacencyList.peekVLong();
    }

    public static <CURSOR extends AdjacencyCursor> boolean isEmpty(CURSOR adjacencyList) {

        return  !adjacencyList.hasNextVLong();

    }



}
