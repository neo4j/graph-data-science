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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class UserInputWriteProperties {

    private UserInputWriteProperties() {}

    public static List<PropertySpec> parse(Object userInput, String configurationKey) {
        var result = new ArrayList<PropertySpec>();

        if (userInput instanceof Iterable) {
            for (Object item : (Iterable) userInput) {
                parseNextInList(item, configurationKey, result);
            }
            return result;
        }
        parseNextInList(userInput, configurationKey, result);
        return result;
    }

    private static void parseNextInList(
        Object userInput,
        String configurationKey,
        ArrayList<PropertySpec> result
    ) {
        if (userInput instanceof String) {
            result.add(new PropertySpec((String) userInput, Optional.empty()));
        } else if (userInput instanceof Map) {
            var convertedMap = (Map<String, String>) userInput;
            for (var entry : convertedMap.entrySet()) {
                var key = entry.getKey();
                var value = entry.getValue();
                result.add(new PropertySpec(key, Optional.of(value)));
            }
        } else {
            throw illegalArgumentException(userInput, configurationKey);
        }
    }

    private static IllegalArgumentException illegalArgumentException(Object userInput, String configurationKey) {
        var type = UserInputAsStringOrListOfString.typeOf(userInput);
        var message = formatWithLocale(
            "Type mismatch for %s: expected List<String> or String or Map<String,String> or List<String or Map<String,String>>, but found %s",
            configurationKey,
            type
        );
        return new IllegalArgumentException(message);
    }


    public static class PropertySpec {
        String nodePropertyName;
        Optional<String> renamedNodeProperty;

        PropertySpec(String nodePropertyName, Optional<String> renamedNodeProperty) {
            this.nodePropertyName = nodePropertyName;
            this.renamedNodeProperty = renamedNodeProperty;
        }

        public String writeProperty() {
            return renamedNodeProperty.orElse(nodePropertyName);
        }

        public String nodeProperty() {
            return nodePropertyName;
        }
    }
}
