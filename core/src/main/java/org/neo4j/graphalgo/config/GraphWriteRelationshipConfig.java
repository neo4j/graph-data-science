/*
 * Copyright (c) 2017-2020 "Neo4j,"
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

import org.neo4j.graphalgo.annotation.Configuration;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.loading.GraphStore;

import java.util.Optional;
import java.util.Set;

import static org.neo4j.graphalgo.utils.StringJoining.join;

@ValueClass
@Configuration("GraphWriteRelationshipConfigImpl")
@SuppressWarnings("immutables:subtype")
public interface GraphWriteRelationshipConfig extends WriteConfig {

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
        if (!graphStore.hasRelationshipType(relationshipType())) {
            throw new IllegalArgumentException(String.format(
                "Relationship type `%s` not found. Available types: %s",
                relationshipType(),
                join(graphStore.relationshipTypes())
            ));
        }
        if (relationshipProperty().isPresent()) {
            Set<String> availableProperties = graphStore.relationshipPropertyKeys(relationshipType());
            String relProperty = relationshipProperty().get();
            if (!availableProperties.contains(relProperty)) {
                throw new IllegalArgumentException(String.format(
                    "Relationship property `%s` not found for relationship type '%s'. Available properties: %s",
                    relProperty,
                    relationshipType(),
                    join(availableProperties)
                ));
            }
        }
    }
}
