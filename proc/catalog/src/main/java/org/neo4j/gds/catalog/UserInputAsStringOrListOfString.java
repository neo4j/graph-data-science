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
package org.neo4j.gds.catalog;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class UserInputAsStringOrListOfString {

    private UserInputAsStringOrListOfString() {}

    public static List<String> parse(Object userInput, String configurationKey) {
        if (userInput instanceof Iterable) {
            var result = new ArrayList<String>();
            for (Object item : (Iterable) userInput) {
                result.add(parseOne(item, configurationKey));
            }
            return result;
        }
        return List.of(parseOne(userInput, configurationKey));
    }

    private static String parseOne(Object userInput, String configurationKey) {
        if (userInput instanceof String) {
            return (String) userInput;
        }
        throw illegalArgumentException(userInput, configurationKey);
    }

    private static IllegalArgumentException illegalArgumentException(Object userInput, String configurationKey) {
        var type = typeOf(userInput);
        var message = formatWithLocale("Type mismatch for %s: expected List<String> or String, but found %s", configurationKey, type);
        return new IllegalArgumentException(message);
    }

    private static String typeOf(Object userInput) {
        if (userInput instanceof Number) return "number";
        if (userInput instanceof Boolean) return "boolean";
        if (userInput instanceof Node) return "node";
        if (userInput instanceof Relationship) return "relationship";
        if (userInput instanceof Path) return "path";
        if (userInput instanceof Map) return "map";
        if (userInput instanceof List) return "list";

        else throw new AssertionError("Developer error, this should not happen");
    }
}
