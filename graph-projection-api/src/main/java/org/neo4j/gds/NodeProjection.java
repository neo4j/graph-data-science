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

import org.immutables.value.Value;
import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.core.ConfigKeyValidation;
import org.neo4j.gds.utils.StringFormatting;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

@ValueClass
public abstract class NodeProjection extends ElementProjection {

    private static final NodeProjection ALL = fromString(PROJECT_ALL);

    public abstract String label();

    @Value.Default
    @Override
    public PropertyMappings properties() {
        return super.properties();
    }

    @Override
    public boolean projectAll() {
        return label().equals(PROJECT_ALL);
    }

    public static final String LABEL_KEY = "label";

    public static NodeProjection of(String label) {
        return ImmutableNodeProjection.of(label, PropertyMappings.of());
    }

    public static NodeProjection all() {
        return ALL;
    }

    public static NodeProjection fromObject(Object object, NodeLabel nodeLabel) {
        if (object instanceof String) {
            return NodeProjection.fromString((String) object);
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
            "Cannot construct a node filter out of a %s",
            object.getClass().getName()
        ));
    }

    public static NodeProjection fromString(@Nullable String label) {
        return NodeProjection.builder().label(label).build();
    }

    public static NodeProjection fromMap(Map<String, Object> map, NodeLabel nodeLabel) {
        validateConfigKeys(map);
        String label = String.valueOf(map.getOrDefault(LABEL_KEY, nodeLabel.name));
        return create(map, properties -> ImmutableNodeProjection.of(label, properties));
    }

    @Override
    boolean includeAggregation() {
        return false;
    }

    @Override
    void writeToObject(Map<String, Object> value) {
        value.put(LABEL_KEY, label());
    }

    @Override
    public NodeProjection withAdditionalPropertyMappings(PropertyMappings mappings) {
        PropertyMappings newMappings = properties().mergeWith(mappings);
        if (newMappings == properties()) {
            return this;
        }
        return ((ImmutableNodeProjection) this).withProperties(newMappings);
    }

    public static Builder builder() {
        return new Builder();
    }

    private static void validateConfigKeys(Map<String, Object> map) {
        ConfigKeyValidation.requireOnlyKeysFrom(List.of(LABEL_KEY, PROPERTIES_KEY), map.keySet());
    }

    @org.immutables.builder.Builder.AccessibleFields
    public static final class Builder extends ImmutableNodeProjection.Builder implements InlineProperties<Builder> {

        private InlinePropertiesBuilder propertiesBuilder;

        Builder() {
        }

        @Override
        public NodeProjection build() {
            buildProperties();
            return super.build();
        }

        @Override
        public InlinePropertiesBuilder inlineBuilder() {
            if (propertiesBuilder == null) {
                propertiesBuilder = new InlinePropertiesBuilder(
                    () -> this.properties,
                    newProperties -> this.properties = newProperties
                );
            }
            return propertiesBuilder;
        }
    }
}
