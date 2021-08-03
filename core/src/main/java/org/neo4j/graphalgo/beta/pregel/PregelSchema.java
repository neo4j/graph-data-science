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
package org.neo4j.graphalgo.beta.pregel;

import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.graphalgo.api.nodeproperties.ValueType;

import java.util.HashSet;
import java.util.Set;

@ValueClass
public interface PregelSchema {

    enum Visibility {
        PUBLIC, PRIVATE
    }

    Set<Element> elements();

    class Builder {

        private final Set<Element> elements = new HashSet<>();

        public PregelSchema.Builder add(String propertyKey, ValueType propertyType) {
            return add(propertyKey, propertyType, Visibility.PUBLIC);
        }

        public PregelSchema.Builder add(String propertyKey, ValueType propertyType, Visibility visibility) {
            elements.add(ImmutableElement.of(propertyKey, propertyType, visibility));
            return this;
        }

        public PregelSchema build() {
            return ImmutablePregelSchema.of(elements);
        }
    }
}
