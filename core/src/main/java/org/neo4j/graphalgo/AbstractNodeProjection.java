/*
 * Copyright (c) 2017-2019 "Neo4j,"
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

import org.immutables.value.Value;
import org.jetbrains.annotations.Nullable;
import org.neo4j.graphalgo.annotation.DataClass;

import java.util.Map;
import java.util.Optional;

@DataClass
@Value.Immutable(singleton = true)
public abstract class AbstractNodeProjection extends ElementProjection {

    public abstract Optional<String> label();

    @Value.Default
    @Override
    public PropertyMappings properties() {
        return super.properties();
    }

    public static final String LABEL_KEY = "label";

    public static NodeProjection empty() {
        return NodeProjection.of();
    }

    public static NodeProjection fromObject(Object object, ElementIdentifier identifier) {
        if (object instanceof String) {
            return NodeProjection.fromString((String) object);
        }
        if (object instanceof Map) {
            @SuppressWarnings("unchecked") Map<String, Object> map = (Map) object;
            return fromMap(map, identifier);
        }
        throw new IllegalArgumentException(String.format(
            "Cannot construct a node filter out of a %s",
            object.getClass().getName()
        ));
    }

    public static NodeProjection fromString(@Nullable String label) {
        return NodeProjection.builder().label(label).build();
    }

    public static NodeProjection fromMap(Map<String, Object> map, ElementIdentifier identifier) {
        String label = String.valueOf(map.getOrDefault(LABEL_KEY, identifier.name));
        return create(map, properties -> NodeProjection.of(label, properties));
    }

    @Override
    boolean includeAggregation() {
        return false;
    }

    @Override
    void writeToObject(Map<String, Object> value) {
        value.put(LABEL_KEY, label().orElse(""));
    }

    @Override
    public NodeProjection withAdditionalPropertyMappings(PropertyMappings mappings) {
        PropertyMappings newMappings = properties().mergeWith(mappings);
        if (newMappings == properties()) {
            return (NodeProjection) this;
        }
        return ((NodeProjection) this).withProperties(newMappings);
    }

    public static Builder builder() {
        return new Builder();
    }

    @org.immutables.builder.Builder.AccessibleFields
    public static final class Builder extends NodeProjection.Builder implements InlineProperties<Builder> {

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
                    newProperties -> {
                        this.properties = newProperties;
                    }
                );
            }
            return propertiesBuilder;
        }
    }
}
