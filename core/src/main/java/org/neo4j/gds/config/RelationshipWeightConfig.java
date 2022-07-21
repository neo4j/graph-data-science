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
package org.neo4j.gds.config;

import org.immutables.value.Value;
import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.utils.StringJoining;

import java.util.Collection;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.neo4j.gds.core.StringIdentifierValidations.emptyToNull;
import static org.neo4j.gds.core.StringIdentifierValidations.validateNoWhiteCharacter;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public interface RelationshipWeightConfig {
    String RELATIONSHIP_WEIGHT_PROPERTY = "relationshipWeightProperty";

    @Value.Default
    @Configuration.ConvertWith("validatePropertyName")
    default @Nullable String relationshipWeightProperty() {
        return null;
    }

    @Value.Derived
    @Configuration.Ignore
    default Optional<String> maybeRelationshipWeightProperty() {
        return Optional.ofNullable(relationshipWeightProperty());
    }

    @Value.Derived
    @Configuration.Ignore
    default boolean hasRelationshipWeightProperty() {
        return relationshipWeightProperty() != null;
    }

    static @Nullable String validatePropertyName(String input) {
        return validateNoWhiteCharacter(emptyToNull(input), RELATIONSHIP_WEIGHT_PROPERTY);
    }

    @Configuration.GraphStoreValidationCheck
    @Value.Default
    default void relationshipWeightValidation(
        GraphStore graphStore,
        Collection<NodeLabel> selectedLabels,
        Collection<RelationshipType> selectedRelationshipTypes
    ) {
        String weightProperty = relationshipWeightProperty();
        if (weightProperty != null) {
            var relTypesWithoutProperty = selectedRelationshipTypes.stream()
                .filter(relType -> !graphStore.hasRelationshipProperty(relType, weightProperty))
                .collect(Collectors.toSet());
            if (!relTypesWithoutProperty.isEmpty()) {
                throw new IllegalArgumentException(formatWithLocale(
                    "Relationship weight property `%s` not found in relationship types %s. Properties existing on all relationship types: %s",
                    weightProperty,
                    StringJoining.join(relTypesWithoutProperty.stream().map(RelationshipType::name)),
                    StringJoining.join(graphStore.relationshipPropertyKeys(selectedRelationshipTypes))
                ));
            }
        }
    }
}
