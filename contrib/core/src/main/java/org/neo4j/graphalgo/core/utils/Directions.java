/*
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
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
package org.neo4j.graphalgo.core.utils;

import org.neo4j.graphdb.Direction;

/**
 * Utility class for converting string representation used in cypher queries
 * to neo4j kernel api Direction type.
 *
 *
 * <p>
 *      String parsing is case insensitive!
 * </p>
 *     <strong>OUTGOING</strong>
 *     <ul>
 *         <li>></li>
 *         <li>o</li>
 *         <li>out</li>
 *         <li>outgoing</li>
 *     </ul>
 *     <strong>INCOMING</strong>
 *     <ul>
 *         <li><</li>
 *         <li>i</li>
 *         <li>in</li>
 *         <li>incoming</li>
 *     </ul>
 *     <strong>BOTH</strong>
 *     <ul>
 *         <li><></li>
 *         <li>b</li>
 *         <li>both</li>
 *     </ul>
 * </p>
 *
 * @author mknblch
 */
public class Directions {

    public static final Direction DEFAULT_DIRECTION = Direction.OUTGOING;

    public static Direction fromString(String directionString) {
        return fromString(directionString, DEFAULT_DIRECTION);
    }

    public static Direction fromString(String directionString, Direction defaultDirection) {

        if (null == directionString) {
            return defaultDirection;
        }

        switch (directionString.toLowerCase()) {

            case "outgoing":
            case "out":
            case "o":
            case ">":
                return Direction.OUTGOING;

            case "incoming":
            case "in":
            case "i":
            case "<":
                return Direction.INCOMING;

            case "both":
            case "b":
            case "<>":
                return Direction.BOTH;

            default:
                return defaultDirection;
        }
    }
}
