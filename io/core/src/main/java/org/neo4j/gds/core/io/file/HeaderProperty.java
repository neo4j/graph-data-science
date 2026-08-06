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
package org.neo4j.gds.core.io.file;

import org.jetbrains.annotations.NotNull;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.api.nodeproperties.ValueTypeToken;

import java.util.OptionalInt;

public record HeaderProperty(
    int position,
    String propertyKey,
    ValueType valueType,
    OptionalInt dimension
) {
    public HeaderProperty(int position, String propertyKey, ValueType valueType) {
        this(position, propertyKey, valueType, OptionalInt.empty());
    }

    public static HeaderProperty parse(int position, String propertyString) {
        String[] propertyArgs = propertyString.split(":");
        if (propertyArgs.length != 2 || propertyArgs[0].isEmpty() || propertyArgs[1].isEmpty()) {
            throw wrongHeaderFormatException(propertyString);
        }
        var token = ValueTypeToken.parse(propertyArgs[1]);
        return new HeaderProperty(position, propertyArgs[0], token.valueType(), token.dimension());
    }

    @NotNull
    private static IllegalArgumentException wrongHeaderFormatException(String propertyString) {
        return new IllegalArgumentException(
            "Header property column does not have expected format <string>:<string>, got " + propertyString);
    }
}
