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
import org.neo4j.gds.ElementProjection;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.utils.StringJoining;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public interface AlgoBaseConfig extends BaseConfig, ConcurrencyConfig, JobIdConfig {

    String NODE_LABELS_KEY = "nodeLabels";
    String RELATIONSHIP_TYPES_KEY = "relationshipTypes";

    @Value.Default
    @Configuration.Key(RELATIONSHIP_TYPES_KEY)
    default List<String> relationshipTypes() {
        return Collections.singletonList(ElementProjection.PROJECT_ALL);
    }

    @Configuration.Ignore
    default Collection<RelationshipType> internalRelationshipTypes(GraphStore graphStore) {
        return ElementIdentityResolver.resolveTypes(graphStore, relationshipTypes());
    }

    @Value.Default
    @Configuration.Key(NODE_LABELS_KEY)
    default List<String> nodeLabels() {
        return Collections.singletonList(ElementProjection.PROJECT_ALL);
    }

    @Configuration.Ignore
    default Collection<NodeLabel> nodeLabelIdentifiers(GraphStore graphStore) {
        return ElementIdentityResolver.resolve(graphStore, nodeLabels());
    }

    @Configuration.GraphStoreValidation
    @Value.Auxiliary
    @Value.Default
    default void graphStoreValidation(
        GraphStore graphStore,
        Collection<NodeLabel> selectedLabels,
        Collection<RelationshipType> selectedRelationshipTypes
    ) {}

    @Configuration.GraphStoreValidationCheck
    default void validateNodeLabels(
        GraphStore graphStore,
        Collection<NodeLabel> selectedLabels,
        Collection<RelationshipType> selectedRelationshipTypes
    ) {
        ElementIdentityResolver.validate(graphStore, selectedLabels, "node labels");
    }

    @Configuration.GraphStoreValidationCheck
    default void validateRelationshipTypes(
        GraphStore graphStore,
        Collection<NodeLabel> selectedLabels,
        Collection<RelationshipType> selectedRelationshipTypes
    ) {
        var availableTypes = graphStore.relationshipTypes();
        var invalidTypes = selectedRelationshipTypes
            .stream()
            .filter(type -> !availableTypes.contains(type))
            .map(RelationshipType::name)
            .collect(Collectors.toList());

        if (!invalidTypes.isEmpty()) {
            throw new IllegalArgumentException(formatWithLocale(
                "Could not find relationship types of %s. Available types are %s.",
                StringJoining.join(invalidTypes.stream()),
                StringJoining.join(availableTypes.stream().map(RelationshipType::name))
            ));
        }
    }
}
