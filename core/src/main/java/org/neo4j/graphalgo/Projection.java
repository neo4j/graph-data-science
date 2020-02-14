/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo;

import org.neo4j.graphdb.Direction;

import java.util.Arrays;
import java.util.stream.Collectors;

public enum Projection {

    NATURAL,
    REVERSE,
    UNDIRECTED;

    public static Projection of(String value) {
        try {
            return Projection.valueOf(value.toUpperCase());
        } catch (IllegalArgumentException e) {
            String availableProjections = Arrays
                .stream(Projection.values())
                .map(Projection::name)
                .collect(Collectors.joining(", "));
            throw new IllegalArgumentException(String.format(
                "Projection `%s` is not supported. Must be one of: %s.",
                value,
                availableProjections));
        }
    }

    public static Direction parseDirection(Object object) {
        if (object == null) {
            return null;
        }
        if (object instanceof String) {
            return Direction.valueOf((String) object);
        }
        if (object instanceof Direction) {
            return (Direction) object;
        }
        return null;
    }
}
