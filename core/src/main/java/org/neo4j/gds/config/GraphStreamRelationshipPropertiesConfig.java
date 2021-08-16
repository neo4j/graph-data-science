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
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.utils.StringFormatting;
import org.neo4j.gds.utils.StringJoining;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

@ValueClass
@Configuration
@SuppressWarnings("immutables:subtype")
public interface GraphStreamRelationshipPropertiesConfig extends BaseConfig {

    @Configuration.Parameter
    Optional<String> graphName();

    @Configuration.Parameter
    List<String> relationshipProperties();

    @Configuration.Parameter
    @Value.Default
    default List<String> relationshipTypes() {
        return Collections.singletonList(ElementProjection.PROJECT_ALL);
    }

    @Configuration.Ignore
    default Collection<RelationshipType> relationshipTypeIdentifiers(GraphStore graphStore) {
        return relationshipTypes().contains(ElementProjection.PROJECT_ALL)
            ? graphStore.relationshipTypes()
            : relationshipTypes().stream().map(RelationshipType::of).collect(Collectors.toList());
    }

    @Value.Default
    default int concurrency() {
        return ConcurrencyConfig.DEFAULT_CONCURRENCY;
    }

    @Configuration.Ignore
    default void validate(GraphStore graphStore) {
        if (!relationshipTypes().contains(ElementProjection.PROJECT_ALL)) {
            // validate that all given labels have all the properties
            relationshipTypeIdentifiers(graphStore).forEach(relationshipType -> {
                    List<String> invalidProperties = relationshipProperties()
                        .stream()
                        .filter(relProperty -> !graphStore.hasRelationshipProperty(relationshipType, relProperty))
                        .collect(Collectors.toList());

                    if (!invalidProperties.isEmpty()) {
                        throw new IllegalArgumentException(formatWithLocale(
                            "Expecting all specified relationship projections to have all given properties defined. " +
                            "Could not find property key(s) %s for label %s. Defined keys: %s.",
                            StringJoining.join(invalidProperties),
                            relationshipType.name,
                            StringJoining.join(graphStore.relationshipPropertyKeys(relationshipType))
                        ));
                    }
                }
            );
        } else {
            // validate that at least one label has all the properties
            boolean hasValidType = relationshipTypeIdentifiers(graphStore).stream()
                .anyMatch(relationshipType -> graphStore
                    .relationshipPropertyKeys(relationshipType)
                    .containsAll(relationshipProperties()));

            if (!hasValidType) {
                throw new IllegalArgumentException(StringFormatting.formatWithLocale(
                    "Expecting at least one relationship projection to contain property key(s) %s.",
                    StringJoining.join(relationshipProperties())
                ));
            }
        }
    }

    /**
     * Returns the relationship types that are to be considered for streaming properties.
     */
    @Configuration.Ignore
    default Collection<RelationshipType> validRelationshipTypes(GraphStore graphStore) {
        return relationshipTypeIdentifiers(graphStore);
    }

    static GraphStreamRelationshipPropertiesConfig of(
        String userName,
        String graphName,
        List<String> nodeProperties,
        List<String> nodeLabels,
        CypherMapWrapper config
    ) {
        return new GraphStreamRelationshipPropertiesConfigImpl(
            Optional.of(graphName),
            nodeProperties,
            nodeLabels,
            userName,
            config
        );
    }

}
