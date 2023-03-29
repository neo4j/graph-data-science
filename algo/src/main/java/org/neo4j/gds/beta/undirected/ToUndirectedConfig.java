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
package org.neo4j.gds.beta.undirected;

import org.immutables.value.Value;
import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.ElementProjection;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.ElementTypeValidator;
import org.neo4j.gds.config.MutateRelationshipConfig;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.utils.StringJoining;

import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;

import static org.neo4j.gds.core.StringIdentifierValidations.emptyToNull;
import static org.neo4j.gds.core.StringIdentifierValidations.validateNoWhiteCharacter;

@Configuration
public interface ToUndirectedConfig extends AlgoBaseConfig, MutateRelationshipConfig {
    @Configuration.ConvertWith(method = "validateRelationshipTypeIdentifier")
    String relationshipType();

    @Value.Default
    @Configuration.ConvertWith(method = "org.neo4j.gds.core.Aggregation#parse")
    @Configuration.ToMapValue("org.neo4j.gds.core.Aggregation#toString")
    Optional<Aggregation> aggregation();

    @Configuration.Ignore
    default RelationshipType internalRelationshipType() {
        return relationshipType().equals(ElementProjection.PROJECT_ALL)
            ? RelationshipType.ALL_RELATIONSHIPS
            : RelationshipType.of(relationshipType());
    }

    @Override
    @Configuration.Ignore
    default List<String> relationshipTypes() {
        return List.of(relationshipType());
    }

    @Override
    @Configuration.Ignore
    default List<String> nodeLabels() {
        return List.of("*");
    }

    static ToUndirectedConfig of(CypherMapWrapper configuration) {
        return new ToUndirectedConfigImpl(configuration);
    }

    @Configuration.GraphStoreValidationCheck
    default void validateTargetRelIsUndirected(
        GraphStore graphStore,
        Collection<NodeLabel> ignored,
        Collection<RelationshipType> ignored_types
    ) {
        RelationshipType type = internalRelationshipType();
        if (graphStore.relationshipTypes().contains(type) && graphStore.schema().relationshipSchema().isUndirected(type)) {
            throw new UnsupportedOperationException(String.format(
                Locale.US,
                "The specified relationship type `%s` is already undirected.",
                type.name
            ));
        }
    }

    @Configuration.GraphStoreValidationCheck
    default void validateStarFilterIsNotAmbiguous(
        GraphStore graphStore,
        Collection<NodeLabel> selectedLabels,
        Collection<RelationshipType> selectedRelationshipTypes
    ) {
        Set<RelationshipType> availableTypes = graphStore.relationshipTypes();

        boolean selectedStar = relationshipType().equals(ElementProjection.PROJECT_ALL);
        boolean projectedStar = availableTypes.contains(RelationshipType.ALL_RELATIONSHIPS);

        if (selectedStar && !projectedStar) {
            throw new IllegalArgumentException(String.format(
                Locale.US,
                "The 'relationshipType' parameter can only be '*' if '*' was projected. Available types are %s.",
                StringJoining.join(availableTypes.stream().map(RelationshipType::name))
            ));
        }
    }

    @Override
    @Configuration.GraphStoreValidationCheck
    default void validateRelationshipTypes(
        GraphStore graphStore,
        Collection<NodeLabel> selectedLabels,
        Collection<RelationshipType> selectedRelationshipTypes
    ) {
        ElementTypeValidator.validateTypes(graphStore, selectedRelationshipTypes, "`relationshipType`");
    }

    static @Nullable String validateRelationshipTypeIdentifier(String input) {
        return validateNoWhiteCharacter(emptyToNull(input), "relationshipType");
    }
}
