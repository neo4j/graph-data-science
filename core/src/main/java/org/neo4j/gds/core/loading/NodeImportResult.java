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
package org.neo4j.gds.core.loading;

import org.immutables.value.Value;
import org.neo4j.gds.PropertyMapping;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.api.PropertyState;
import org.neo4j.gds.api.properties.nodes.NodeProperty;
import org.neo4j.gds.api.properties.nodes.NodePropertyStore;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;

import java.util.Map;

@ValueClass
public interface NodeImportResult {

    IdMap idMap();

    @Value.Default
    default NodePropertyStore properties() {
        return NodePropertyStore.empty();
    }

    static NodeImportResult of(IdMap idmap) {
        return ImmutableNodeImportResult.of(idmap, NodePropertyStore.empty());
    }

    static NodeImportResult of(IdMap idmap, NodePropertyStore nodePropertyStore) {
        return ImmutableNodeImportResult.of(idmap, nodePropertyStore);
    }

    static NodeImportResult of(IdMap idMap, Map<PropertyMapping, NodePropertyValues> properties) {
        NodePropertyStore.Builder builder = NodePropertyStore.builder();
        properties.forEach((mapping, nodeProperties) -> builder.putProperty(
            mapping.propertyKey(),
            NodeProperty.of(
                mapping.propertyKey(),
                PropertyState.PERSISTENT,
                nodeProperties,
                mapping.defaultValue().isUserDefined()
                    ? mapping.defaultValue()
                    : nodeProperties.valueType().fallbackValue()
            )
        ));
        return ImmutableNodeImportResult.of(idMap, builder.build());
    }
}
