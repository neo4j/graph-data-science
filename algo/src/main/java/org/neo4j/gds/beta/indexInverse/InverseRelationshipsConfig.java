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
package org.neo4j.gds.beta.indexInverse;

import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.ElementProjection;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.schema.RelationshipSchema;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.utils.StringJoining;

import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

import static org.neo4j.gds.core.StringIdentifierValidations.emptyToNull;
import static org.neo4j.gds.core.StringIdentifierValidations.validateNoWhiteCharacter;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

@Configuration
public interface InverseRelationshipsConfig extends AlgoBaseConfig {
    static @Nullable List<String> parseRelTypes(Object input) {
        if (input instanceof String) {
            var strInput = ((String) input);
            validateNoWhiteCharacter(emptyToNull(strInput), "relationshipType");

            return List.of(strInput);
        }

        if (input instanceof List) {
            return ((List<?>) input).stream().flatMap(i -> parseRelTypes(i).stream()).collect(Collectors.toList());
        }

        throw new IllegalArgumentException(formatWithLocale(
            "Expected relationship type to be a String. Got %s.",
            input.getClass().getSimpleName()
        ));
    }

    @Override
    @Configuration.ConvertWith(method = "parseRelTypes")
    List<String> relationshipTypes();

    @Override
    @Configuration.Ignore
    default List<String> nodeLabels() {
        return List.of("*");
    }

    static InverseRelationshipsConfig of(CypherMapWrapper configuration) {
        return new InverseRelationshipsConfigImpl(configuration);
    }

    @Configuration.GraphStoreValidationCheck
    default void validateNotIndexed(
        GraphStore graphStore,
        Collection<NodeLabel> selectedLabels,
        Collection<RelationshipType> selectedRelationshipTypes
    ) {
        Set<RelationshipType> indexTypes = graphStore.inverseIndexedRelationshipTypes();
        var alreadyIndexedTypes = selectedRelationshipTypes
            .stream()
            .filter(indexTypes::contains)
            .map(RelationshipType::name)
            .collect(Collectors.toList());

        if (!alreadyIndexedTypes.isEmpty()) {
            throw new UnsupportedOperationException(String.format(Locale.US, "Inverse index already exists for %s.",
                StringJoining.join(alreadyIndexedTypes)
            ));
        }
    }

    @Configuration.GraphStoreValidationCheck
    default void validateNotUndirected(
        GraphStore graphStore,
        Collection<NodeLabel> selectedLabels,
        Collection<RelationshipType> selectedRelationshipTypes
    ) {
        RelationshipSchema relationshipSchema = graphStore.schema().relationshipSchema();

        var undirectedTypes = selectedRelationshipTypes
            .stream()
            .filter(relationshipSchema::isUndirected)
            .map(RelationshipType::name)
            .collect(Collectors.toList());

        if (!undirectedTypes.isEmpty()) {
            throw new UnsupportedOperationException(String.format(
                Locale.US,
                "Creating an inverse index for undirected relationships is not supported. Undirected relationship types are %s.",
                StringJoining.join(undirectedTypes)
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

        boolean selectedStar = relationshipTypes().contains(ElementProjection.PROJECT_ALL);
        boolean projectedStar = availableTypes.contains(RelationshipType.ALL_RELATIONSHIPS);
        boolean notOnlyStar = availableTypes.size() > 1;

        if (selectedStar && projectedStar && notOnlyStar) {
            throw new IllegalArgumentException(String.format(
                Locale.US,
                "The 'relationshipTypes' parameter is ambiguous. It is not clear whether all relationships types or only the star projection should be used. " +
                "Please explicitly enumerate the requested types. Available types are %s.",
                StringJoining.join(availableTypes.stream().map(RelationshipType::name))
            ));
        }
    }
}
