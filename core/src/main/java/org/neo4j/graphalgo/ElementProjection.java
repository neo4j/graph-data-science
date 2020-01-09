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

import org.immutables.value.Value;
import org.jetbrains.annotations.Nullable;
import org.neo4j.graphalgo.core.DeduplicationStrategy;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.util.Collections.emptyMap;

public abstract class ElementProjection {

    public static final String PROPERTIES_KEY = "properties";

    @Value.Default
    @Value.Parameter
    public PropertyMappings properties() {
        return PropertyMappings.of();
    }

    public abstract ElementProjection withAdditionalPropertyMappings(PropertyMappings mappings);

    public final Map<String, Object> toObject() {
        Map<String, Object> value = new LinkedHashMap<>();
        writeToObject(value);
        value.put(PROPERTIES_KEY, properties().toObject(includeAggregation()));
        return value;
    }

    static <T extends ElementProjection> T create(
        Map<String, Object> config,
        Function<PropertyMappings, T> constructor
    ) {
        Object properties = config.getOrDefault(PROPERTIES_KEY, emptyMap());
        PropertyMappings propertyMappings = PropertyMappings.fromObject(properties);
        return constructor.apply(propertyMappings);
    }

    static String nonEmptyString(Map<String, Object> config, String key) {
        @Nullable Object value = config.get(key);
        if (!(value instanceof String) || ((String) value).isEmpty()) {
            throw new IllegalArgumentException(String.format(
                "'%s' is not a valid value for  the key '%s'",
                value, key
            ));
        }
        return (String) value;
    }

    abstract void writeToObject(Map<String, Object> value);

    abstract boolean includeAggregation();

    interface InlineProperties<Self extends InlineProperties<Self>> {

        default Self addProperty(PropertyMapping mapping) {
            inlineBuilder().propertiesBuilder().addMapping(mapping);
            return (Self) this;
        }

        default Self addProperty(
            @Nullable String propertyKey,
            @Nullable String neoPropertyKey,
            double defaultValue
        ) {
            return this.addProperty(propertyKey, neoPropertyKey, defaultValue, DeduplicationStrategy.DEFAULT);
        }

        default Self addProperty(
            @Nullable String propertyKey,
            @Nullable String neoPropertyKey,
            double defaultValue,
            DeduplicationStrategy deduplicationStrategy
        ) {
            inlineBuilder().propertiesBuilder().addMapping(propertyKey, neoPropertyKey, defaultValue, deduplicationStrategy);
            return (Self) this;
        }

        default Self addProperties(PropertyMapping... properties) {
            inlineBuilder().propertiesBuilder().addMappings(properties);
            return (Self) this;
        }

        default Self addAllProperties(Iterable<? extends PropertyMapping> properties) {
            inlineBuilder().propertiesBuilder().addAllMappings(properties);
            return (Self) this;
        }

        default Self addPropertyMappings(PropertyMappings propertyMappings) {
            return addAllProperties(propertyMappings.mappings());
        }

        default void buildProperties() {
            inlineBuilder().build();
        }

        InlinePropertiesBuilder inlineBuilder();
    }

    static final class InlinePropertiesBuilder {
        private final Supplier<PropertyMappings> getProperties;
        private final Consumer<PropertyMappings> setProperties;
        private AbstractPropertyMappings.Builder propertiesBuilder;

        InlinePropertiesBuilder(
            Supplier<PropertyMappings> getProperties,
            Consumer<PropertyMappings> setProperties
        ) {
            this.getProperties = getProperties;
            this.setProperties = setProperties;
        }

        private void build() {
            if (propertiesBuilder != null) {
                if (getProperties.get() != null) {
                    throw new IllegalStateException(
                        "Cannot have both, a complete mapping from `properties` " +
                        "and other properties from `addProperty`. If you want to " +
                        "combine those, make sure to call `properties` first and " +
                        "then use `addProperty` and never set a new `properties`" +
                        "again."
                    );
                }
                setProperties.accept(propertiesBuilder.build());
            }
        }

        private AbstractPropertyMappings.Builder propertiesBuilder() {
            if (propertiesBuilder == null) {
                propertiesBuilder = AbstractPropertyMappings.builder();
                PropertyMappings properties = getProperties.get();
                if (properties != null) {
                    propertiesBuilder.from(properties);
                    setProperties.accept(null);
                }
            }
            return propertiesBuilder;
        }
    }
}
