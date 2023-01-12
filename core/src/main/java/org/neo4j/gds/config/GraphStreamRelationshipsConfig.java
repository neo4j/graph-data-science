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
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.CypherMapWrapper;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

@Configuration
public interface GraphStreamRelationshipsConfig extends BaseConfig, ConcurrencyConfig  {

    @Configuration.Parameter
    Optional<String> graphName();

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

    @Configuration.Ignore
    default void validate(GraphStore graphStore) {
        var missingRelationshipTypes = new ArrayList<RelationshipType>();
        relationshipTypeIdentifiers(graphStore).forEach(relationshipType -> {
            if (!graphStore.hasRelationshipType(relationshipType)) {
                missingRelationshipTypes.add(relationshipType);
            }
        });

        if (!missingRelationshipTypes.isEmpty()) {
            throw new IllegalStateException(formatWithLocale(
                "Expecting all specified relationship types to be present in graph store, but could not find %s",
                missingRelationshipTypes
            ));
        }
    }

    static GraphStreamRelationshipsConfig of(
        String graphName,
        List<String> relationshipTypes,
        CypherMapWrapper config
    ) {
        return new GraphStreamRelationshipsConfigImpl(
            Optional.of(graphName),
            relationshipTypes,
            config
        );
    }
}
