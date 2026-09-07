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
package org.neo4j.gds;

import org.neo4j.gds.core.ConfigKeyValidation;
import org.neo4j.gds.utils.StringFormatting;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static java.util.Collections.emptyMap;

public record NodeProjection(String label, PropertyMappings properties) implements ElementProjection {
    public NodeProjection(String label) {
        this(label, PropertyMappings.of());
    }

    private static final NodeProjection ALL = new NodeProjection(PROJECT_ALL);

    public static final String LABEL_KEY = "label";

    public static NodeProjection all() {
        return ALL;
    }

    public static NodeProjection fromObject(Object object, NodeLabel nodeLabel) {
        if (object instanceof String) {
            return new NodeProjection((String) object);
        }
        if (object instanceof Map) {
            var caseInsensitiveMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            //noinspection unchecked
            caseInsensitiveMap.putAll((Map<String, Object>) object);

            return fromMap(caseInsensitiveMap, nodeLabel);
        }
        if (object instanceof NodeProjection) {
            return ((NodeProjection) object);
        }

        throw new IllegalArgumentException(StringFormatting.formatWithLocale(
            "Cannot construct a node projection out of a %s",
            object.getClass().getName()
        ));
    }

    public static NodeProjection fromMap(Map<String, Object> map, NodeLabel nodeLabel) {
        validateConfigKeys(map);
        var label = String.valueOf(map.getOrDefault(LABEL_KEY, nodeLabel.name));
        var inputProperties = map.getOrDefault(PROPERTIES_KEY, emptyMap());
        var properties = PropertyMappings.fromObject(inputProperties);
        return new NodeProjection(label, properties);
    }

    public Map<String, Object> toObject() {
        Map<String, Object> value = new LinkedHashMap<>();
        value.put(LABEL_KEY, label);
        value.put(PROPERTIES_KEY, properties().toObject(false));
        return value;
    }

    NodeProjection withAdditionalPropertyMappings(PropertyMappings mappings) {
        PropertyMappings newMappings = properties.mergeWith(mappings);
        if (newMappings == properties) {
            return this;
        }
        return new NodeProjection(label, newMappings);
    }

    private static void validateConfigKeys(Map<String, Object> map) {
        ConfigKeyValidation.requireOnlyKeysFrom(List.of(LABEL_KEY, PROPERTIES_KEY), map.keySet());
    }
}
