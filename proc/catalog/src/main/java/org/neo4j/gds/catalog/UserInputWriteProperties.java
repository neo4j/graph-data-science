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
        Object originalUserInput = userInput;
        if (userInput instanceof Iterable) {
            for (Object item : (Iterable) userInput) {
                parseNextInList(item, configurationKey, result, originalUserInput);
            }
            return result;
        }
        parseNextInList(userInput, configurationKey, result, originalUserInput);
        return result;
    }

    private static void parseNextInList(
        Object userInput,
        String configurationKey,
        ArrayList<PropertySpec> result,
        Object originalUserInput
    ) {
        if (userInput instanceof String) {
            result.add(new PropertySpec((String) userInput, Optional.empty()));
        } else if (userInput instanceof Map) {
            var convertedMap = (Map) userInput;
            for (var entry : convertedMap.entrySet()) {
                if (entry instanceof Map.Entry<?, ?>) {
                    var key = ((Map.Entry<?, ?>) entry).getKey();
                    var value = ((Map.Entry<?, ?>) entry).getValue();

                    if (key instanceof String && value instanceof String) {
                        var stringKey = (String) key;
                        var stringValue = (String) value;
                        result.add(new PropertySpec(stringKey, Optional.of(stringValue)));
                    } else {
                        throw illegalArgumentException(originalUserInput, configurationKey);
                    }
                }
            }
        } else {
            throw illegalArgumentException(originalUserInput, configurationKey);
        }
    }

    private static IllegalArgumentException illegalArgumentException(Object userInput, String configurationKey) {
        var type = UserInputAsStringOrListOfString.typeOf(userInput);
        if (type.equals("map") || type.equals("list")) {
            type = "improperly defined " + type;
        }
      
        var message = formatWithLocale(
            "Type mismatch for %s: expected String, Map<String,String>, or List<String and/or Map<String,String>>, but found %s",
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
