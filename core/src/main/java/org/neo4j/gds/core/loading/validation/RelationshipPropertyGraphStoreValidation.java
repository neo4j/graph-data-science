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
import org.neo4j.gds.utils.StringJoining;

import java.util.Collection;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.neo4j.gds.config.RelationshipWeightConfig.RELATIONSHIP_WEIGHT_PROPERTY;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class RelationshipPropertyGraphStoreValidation implements GraphStoreFilteredETL {
    private final Optional<String> propertyName;
    private final String parameterName;

    public RelationshipPropertyGraphStoreValidation(Optional<String> propertyName, String parameterName) {
        this.propertyName = propertyName;
        this.parameterName = parameterName;
    }

    public RelationshipPropertyGraphStoreValidation(Optional<String> propertyName) {
        this.propertyName = propertyName;
        this.parameterName = RELATIONSHIP_WEIGHT_PROPERTY;
    }

    @Override
    public void onGraphStoreFiltered(
        GraphStore graphStore,
        Collection<NodeLabel> selectedLabels,
        Collection<RelationshipType> selectedRelationshipTypes,
        Optional<String> relationshipProperty
    ) {
        if (propertyName.isEmpty()) return;

        var propertyNameAsString = propertyName.get();

        var relTypesWithoutProperty = selectedRelationshipTypes.stream()
            .filter(relType -> !graphStore.hasRelationshipProperty(relType, propertyNameAsString))
            .collect(Collectors.toSet());

        if (!relTypesWithoutProperty.isEmpty()) {
            var parameterOutput = formatParameterOutput(propertyNameAsString);

            throw new IllegalArgumentException(formatWithLocale(
                "%s not found in relationship types %s. Properties existing on all relationship types: %s",
                parameterOutput,
                StringJoining.join(relTypesWithoutProperty.stream().map(RelationshipType::name)),
                StringJoining.join(graphStore.relationshipPropertyKeys(selectedRelationshipTypes))
            ));
        }
    }

    private String formatParameterOutput(String propertyNameAsString) {
        if (parameterName.equals(RELATIONSHIP_WEIGHT_PROPERTY)) return formatWithLocale(
            "Relationship weight property `%s`",
            propertyNameAsString
        );

        return formatWithLocale(
            "Relationship property `%s` for parameter `%s`",
            propertyNameAsString,
            parameterName
        );
    }
}
