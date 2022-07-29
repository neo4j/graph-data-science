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
package org.neo4j.gds.core.io.file.csv;

import org.neo4j.gds.ElementIdentifier;
import org.neo4j.gds.RelationshipType;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public final class CsvMapUtil {

    private static final char LIST_DELIMITER = ';';

    private CsvMapUtil() {}

    public static String relationshipCountsToString(Map<RelationshipType, Long> map) {
        return toString(map, ElementIdentifier::name, l -> Long.toString(l));
    }

    static <KEY, VALUE> Map<KEY, VALUE> fromString(
        String mapString,
        Function<String, KEY> keyParser,
        Function<String, VALUE> valueParser
    ) {
        var listElements = mapString.split(String.valueOf(LIST_DELIMITER));
        var map = new HashMap<KEY, VALUE>();
        for (int i = 0; i < listElements.length; i+=2) {
            map.put(keyParser.apply(listElements[i]), valueParser.apply(listElements[i + 1]));
        }
        return map;
    }

    private static <KEY, VALUE> String toString(Map<KEY, VALUE> map, Function<KEY, String> keySerializer, Function<VALUE, String> valueSerializer) {
        var stringBuilder = new StringBuilder();
        map.keySet().stream().sorted(Comparator.comparing(keySerializer, Comparator.naturalOrder())).forEach(key -> {
            stringBuilder.append(keySerializer.apply(key));
            stringBuilder.append(LIST_DELIMITER);
            stringBuilder.append(valueSerializer.apply(map.get(key)));
            stringBuilder.append(LIST_DELIMITER);
        });
        return stringBuilder.toString();
    }
}
