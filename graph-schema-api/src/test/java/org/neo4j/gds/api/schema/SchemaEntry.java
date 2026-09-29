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
package org.neo4j.gds.api.schema;

import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.PropertyState;
import org.neo4j.gds.api.nodeproperties.ValueType;

import java.util.Optional;

final class SchemaEntry {
    private final String label;
    private final Optional<String> maybePropertyKey;
    private final Optional<ValueType> maybeValueType;
    private final Optional<DefaultValue> maybeDefaultValue;
    private final Optional<PropertyState> maybePropertyState;

    private SchemaEntry(
        String label,
        Optional<String> maybePropertyKey,
        Optional<ValueType> maybeValueType,
        Optional<DefaultValue> maybeDefaultValue,
        Optional<PropertyState> maybePropertyState
    ) {
        this.label = label;
        this.maybePropertyKey = maybePropertyKey;
        this.maybeValueType = maybeValueType;
        this.maybeDefaultValue = maybeDefaultValue;
        this.maybePropertyState = maybePropertyState;
    }

    static SchemaEntry of(String label) {
        return new SchemaEntry(label, Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
    }

    static SchemaEntry of(String label, String propertyKey, ValueType valueType) {
        return new SchemaEntry(
            label,
            Optional.of(propertyKey),
            Optional.of(valueType),
            Optional.empty(),
            Optional.empty()
        );
    }

    static SchemaEntry of(
        String label,
        String propertyKey,
        ValueType valueType,
        DefaultValue defaultValue,
        PropertyState propertyState
    ) {
        return new SchemaEntry(
            label,
            Optional.of(propertyKey),
            Optional.of(valueType),
            Optional.of(defaultValue),
            Optional.of(propertyState));
    }

    String label() { return label; }

    Optional<NodePropertySchema> toPropertySchema() {
        if (maybePropertyKey.isEmpty() || maybeValueType.isEmpty()) {
            return Optional.empty();
        } else if (maybeDefaultValue.isEmpty() || maybePropertyState.isEmpty()) {
            return Optional.of(NodePropertySchema.of(maybePropertyKey.get(), maybeValueType.get()));
        } else {
            return Optional.of(NodePropertySchema.of(
                maybePropertyKey.get(),
                maybeValueType.get(),
                maybeDefaultValue.get(),
                maybePropertyState.get()
            ));
        }
    }
}
