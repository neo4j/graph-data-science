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

import org.neo4j.gds.PropertyMapping;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.api.NodeProperties;
import org.neo4j.gds.api.NodeProperty;
import org.neo4j.gds.api.NodePropertyStore;
import org.neo4j.gds.api.PropertyState;

import java.util.Map;

@ValueClass
public interface IdMapAndProperties {

    IdMap idMap();

    NodePropertyStore properties();

    static IdMapAndProperties of(IdMap idMap, Map<PropertyMapping, NodeProperties> properties) {
        NodePropertyStore.Builder builder = NodePropertyStore.builder();
        properties.forEach((mapping, nodeProperties) -> builder.putNodeProperty(
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
        return ImmutableIdMapAndProperties.of(idMap, builder.build());
    }
}
