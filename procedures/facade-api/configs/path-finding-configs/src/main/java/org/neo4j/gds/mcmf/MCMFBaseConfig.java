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
package org.neo4j.gds.mcmf;

import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.maxflow.MaxFlowBaseConfig;
import org.neo4j.gds.utils.StringJoining;

import java.util.Collection;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.neo4j.gds.core.StringIdentifierValidations.emptyToNull;
import static org.neo4j.gds.core.StringIdentifierValidations.validateNoWhiteCharacter;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

@Configuration
public interface MCMFBaseConfig extends MaxFlowBaseConfig {

     String COST_PROPERTY ="costProperty";

    @Override
    @Configuration.Key("capacityProperty")
    Optional<String> relationshipWeightProperty();

    Optional<String> costProperty();
    @Configuration.IntegerRange(min = 2)

    default int alpha(){ return 6;}

    @Configuration.Ignore
    default MCMFParameters toMCMFParameters() {
        return new MCMFParameters(
            toMaxFlowParameters(),
            alpha()
        );
    }

    @Configuration.Check
    default void validateCostProperty() {
        relationshipWeightProperty().ifPresent(input -> validateNoWhiteCharacter(
            emptyToNull(input),
            COST_PROPERTY
        ));
    }

    @Configuration.GraphStoreValidationCheck
    default void costValidation(
        GraphStore graphStore,
        Collection<NodeLabel> selectedLabels,
        Collection<RelationshipType> selectedRelationshipTypes
    ) {
        relationshipWeightProperty().ifPresent(prop -> {
            var relTypesWithoutProperty = selectedRelationshipTypes.stream()
                .filter(relType -> !graphStore.hasRelationshipProperty(relType, prop))
                .collect(Collectors.toSet());
            if (!relTypesWithoutProperty.isEmpty()) {
                throw new IllegalArgumentException(formatWithLocale(
                    "Relationship property `%s` for parameter `%s not found in relationship types %s. Properties existing on all relationship types: %s",
                    prop,
                    COST_PROPERTY,
                    StringJoining.join(relTypesWithoutProperty.stream().map(RelationshipType::name)),
                    StringJoining.join(graphStore.relationshipPropertyKeys(selectedRelationshipTypes))
                ));
            }
        });
    }

}
