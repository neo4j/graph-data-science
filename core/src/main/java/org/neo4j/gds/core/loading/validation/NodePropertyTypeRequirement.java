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
package org.neo4j.gds.core.loading.validation;

import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.utils.StringFormatting;
import org.neo4j.gds.utils.StringJoining;

import java.util.Collection;
import java.util.List;

public class NodePropertyTypeRequirement implements AlgorithmGraphStoreRequirements {
    private final String nodeProperty;
    private final List<ValueType> allowedValueTypes;

    public NodePropertyTypeRequirement(String nodeProperty, List<ValueType> allowedValueTypes) {
        this.nodeProperty = nodeProperty;
        this.allowedValueTypes = allowedValueTypes;
    }

    @Override
    public void validate(
        GraphStore graphStore,
        Collection<NodeLabel> selectedLabels,
        Collection<RelationshipType> selectedRelationshipTypes
    ) {
        validatePropertyType(graphStore);

    }

    void validatePropertyType(GraphStore graphStore) {
        var valueType = graphStore.nodeProperty(nodeProperty).valueType();
        for (var currentValueType : allowedValueTypes) {
            if (valueType == currentValueType) {
                return;
            }
        }
        throw new IllegalArgumentException(
            StringFormatting.formatWithLocale(
                "Unsupported node property value type for property `%s`: %s. Value types accepted: %s",
                nodeProperty,
                valueType,
                StringJoining.join(allowedValueTypes.stream().map(Enum::name))
            )
        );
    }
}
