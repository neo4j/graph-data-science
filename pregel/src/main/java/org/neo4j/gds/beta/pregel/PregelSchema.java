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
package org.neo4j.gds.beta.pregel;

import org.immutables.value.Value;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.values.GdsValue;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@ValueClass
public interface PregelSchema {

    enum Visibility {
        PUBLIC, PRIVATE
    }

    Set<Element> elements();

    @Value.Auxiliary
    default Map<String, ValueType> propertiesMap() {
        return elements().stream().collect(Collectors.toMap(Element::propertyKey, Element::propertyType));
    }

    class Builder {

        private final Set<Element> elements = new HashSet<>();

        public PregelSchema.Builder add(String propertyKey, ValueType propertyType) {
            return add(propertyKey, propertyType, Visibility.PUBLIC);
        }

        public PregelSchema.Builder add(String propertyKey, ValueType propertyType, Visibility visibility) {
            this.elements.add(ImmutableElement.builder()
                .propertyKey(propertyKey)
                .propertyType(propertyType)
                .visibility(visibility)
                .build());
            return this;
        }

        public PregelSchema.Builder add(String propertyKey, GdsValue defaultValue, Visibility visibility) {
            this.elements.add(ImmutableElement.builder()
                .propertyKey(propertyKey)
                .propertyType(defaultValue.type())
                .defaultValue(defaultValue)
                .visibility(visibility)
                .build());
            return this;
        }

        public PregelSchema build() {
            return ImmutablePregelSchema.of(elements);
        }
    }
}
