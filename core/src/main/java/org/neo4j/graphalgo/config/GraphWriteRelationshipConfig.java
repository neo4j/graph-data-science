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
package org.neo4j.graphalgo.config;

import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.graphalgo.RelationshipType;
import org.neo4j.graphalgo.api.GraphStore;

import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;
import static org.neo4j.graphalgo.utils.StringJoining.join;

@ValueClass
@Configuration
@SuppressWarnings("immutables:subtype")
public interface GraphWriteRelationshipConfig extends AlgoBaseConfig, WriteConfig {

    @Configuration.Parameter
    String relationshipType();

    @Configuration.Parameter
    Optional<String> relationshipProperty();

    static GraphWriteRelationshipConfig of(
        String userName,
        String graphName,
        String relationshipType,
        Optional<String> relationshipProperty,
        CypherMapWrapper config
    ) {
        return new GraphWriteRelationshipConfigImpl(
            relationshipType,
            relationshipProperty,
            Optional.of(graphName),
            Optional.empty(),
            userName,
            config
        );
    }

    @Configuration.Ignore
    default void validate(GraphStore graphStore) {
        if (!graphStore.hasRelationshipType(RelationshipType.of(relationshipType()))) {
            throw new IllegalArgumentException(formatWithLocale(
                "Relationship type `%s` not found. Available types: %s",
                relationshipType(),
                join(graphStore.relationshipTypes().stream().map(RelationshipType::name).collect(Collectors.toSet()))
            ));
        }
        if (relationshipProperty().isPresent()) {
            Set<String> availableProperties = graphStore.relationshipPropertyKeys(RelationshipType.of(relationshipType()));
            String relProperty = relationshipProperty().get();
            if (!availableProperties.contains(relProperty)) {
                throw new IllegalArgumentException(formatWithLocale(
                    "Relationship property `%s` not found for relationship type '%s'. Available properties: %s",
                    relProperty,
                    relationshipType(),
                    join(availableProperties)
                ));
            }
        }
    }
}
