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
package org.neo4j.gds.beta.filter.expression;

import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.nodeproperties.ValueType;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public record ValidationContext(
    Context context,
    Set<NodeLabel> availableNodeLabels,
    Set<RelationshipType> availableRelationshipTypes,
    Map<String, ValueType> availableProperties,
    List<SemanticErrors.SemanticError> semanticError
) {

    public ValidationContext withError(SemanticErrors.SemanticError error) {
        var errors = new ArrayList<SemanticErrors.SemanticError>();
        errors.add(error);
        errors.addAll(semanticError);
        return new ValidationContext(context, availableNodeLabels, availableRelationshipTypes, availableProperties, errors);
    }

    public void validate() throws SemanticErrors {
        if (!semanticError.isEmpty()) {
            throw SemanticErrors.of(semanticError);
        }
    }

    public static ValidationContext forNodes(GraphStore graphStore) {
        var propertiesAndTypes = graphStore
            .schema()
            .nodeSchema()
            .properties()
            .entrySet()
            .stream()
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                e -> e.getValue().valueType()
            ));

        return new ValidationContext(
            Context.NODE,
            new HashSet<>(graphStore.nodeLabels()),
            Set.of(),
            propertiesAndTypes,
            List.of()
        );
    }

    public static ValidationContext forRelationships(GraphStore graphStore) {
        var propertiesAndTypes = graphStore
            .schema()
            .relationshipSchema()
            .unionProperties()
            .entrySet()
            .stream()
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                e -> e.getValue().valueType()
            ));

        return new ValidationContext(Context.RELATIONSHIP, Set.of(), new HashSet<>(graphStore.relationshipTypes()), propertiesAndTypes, List.of());
    }

    enum Context {
        NODE,
        RELATIONSHIP
    }
}
