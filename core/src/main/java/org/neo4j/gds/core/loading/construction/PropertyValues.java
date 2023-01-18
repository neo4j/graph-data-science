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
package org.neo4j.gds.core.loading.construction;

import org.neo4j.gds.core.loading.ValueConverter;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.MapValue;

import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

public abstract class PropertyValues {

    public abstract void forEach(BiConsumer<String, Value> consumer);

    public abstract boolean isEmpty();

    public abstract Iterable<String> propertyKeys();

    public static PropertyValues of(MapValue mapValue) {
        return new CypherPropertyValues(mapValue);
    }

    public static PropertyValues of(Map<String, Value> map) {
        return new NativePropertyValues(map);
    }

    private static final class NativePropertyValues extends PropertyValues {
        private final Map<String, Value> properties;

        private NativePropertyValues(Map<String, Value> properties) {
            this.properties = properties;
        }

        @Override
        public void forEach(BiConsumer<String, Value> consumer) {
            this.properties.forEach(consumer);
        }

        @Override
        public boolean isEmpty() {
            return this.properties.isEmpty();
        }

        @Override
        public Set<String> propertyKeys() {
            return this.properties.keySet();
        }
    }

    private static final class CypherPropertyValues extends PropertyValues {
        private final MapValue properties;

        private CypherPropertyValues(MapValue properties) {
            this.properties = properties;
        }

        @Override
        public void forEach(BiConsumer<String, Value> consumer) {
            this.properties.foreach((k, v) -> {
                if (v != Values.NO_VALUE) {
                    consumer.accept(k, ValueConverter.toValue(v));
                }
            });
        }

        @Override
        public boolean isEmpty() {
            return this.properties.isEmpty();
        }

        @Override
        public Iterable<String> propertyKeys() {
            return this.properties.keySet();
        }
    }
}
