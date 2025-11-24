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
import org.neo4j.gds.config.ElementTypeValidator;

import java.util.Collection;
import java.util.Optional;
import java.util.Set;

public class GraphStoreValidation {

    private final AlgorithmGraphStoreRequirements algorithmRequirements;

    public GraphStoreValidation(AlgorithmGraphStoreRequirements algorithmRequirements) {
        this.algorithmRequirements = algorithmRequirements;
    }

    public void validate(
        GraphStore graphStore,
        Collection<NodeLabel> selectedLabels,
        Collection<RelationshipType> selectedRelationshipTypes,
        Optional<String> relationshipProperty
    ) {
        validateNodeLabels(graphStore, selectedLabels);
        validateRelationshipTypes(graphStore, selectedRelationshipTypes);
        validateRelationshipProperty(graphStore, selectedRelationshipTypes, relationshipProperty);
        validateAlgorithmRequirements(graphStore, selectedLabels, selectedRelationshipTypes);
    }

    protected void validateNodeLabels(
        GraphStore graphStore,
        Collection<NodeLabel> selectedLabels
    ) {
        ElementTypeValidator.validate(graphStore, selectedLabels, "`nodeLabels`");
    }

    protected void validateRelationshipTypes(
        GraphStore graphStore,
        Collection<RelationshipType> selectedRelationshipTypes
    ) {
        ElementTypeValidator.validateTypes(graphStore, selectedRelationshipTypes, "`relationshipTypes`");
    }

    protected void validateRelationshipProperty(
        GraphStore graphStore,
        Collection<RelationshipType> selectedRelationshipTypes,
        Optional<String> relationshipProperty
    ) {
        new RelationshipPropertyGraphStoreValidation(
            relationshipProperty
        ).validate(graphStore, Set.of(),selectedRelationshipTypes);

    }

    protected void validateAlgorithmRequirements(
        GraphStore graphStore,
        Collection<NodeLabel> selectedLabels,
        Collection<RelationshipType> selectedRelationshipTypes
    ) {
        algorithmRequirements.validate(graphStore, selectedLabels, selectedRelationshipTypes);
    }

}
