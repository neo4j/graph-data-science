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
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.nodeproperties.ValueType;

@ValueClass
public interface HeaderProperty {
    int position();

    String propertyKey();

    ValueType valueType();

    static HeaderProperty parse(int position, String propertyString) {
        String[] propertyArgs = propertyString.split(":");
        if (propertyArgs.length != 2) {
            throw wrongHeaderFormatException(propertyString);
        } else if (propertyArgs[0].isEmpty() || propertyArgs[1].isEmpty()) {
            throw wrongHeaderFormatException(propertyString);
        }
        return ImmutableHeaderProperty.of(position, propertyArgs[0], ValueType.fromCsvName(propertyArgs[1]));
    }

    @NotNull
    private static IllegalArgumentException wrongHeaderFormatException(String propertyString) {
        return new IllegalArgumentException(
            "Header property column does not have expected format <string>:<string>, got " + propertyString);
    }
}
