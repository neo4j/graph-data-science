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
package org.neo4j.graphalgo.catalog;

import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.RelationshipProjection;
import org.neo4j.graphalgo.RelationshipType;
import org.neo4j.graphalgo.config.DeleteRelationshipsConfig;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.core.loading.DeletionResult;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;
import org.neo4j.graphalgo.core.loading.GraphStoreWithConfig;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;

public class GraphDeleteRelationshipProc extends CatalogProc {

    private static final String DESCRIPTION = "";

    @Procedure(name = "gds.graph.deleteRelationships", mode = READ)
    @Description(DESCRIPTION)
    public Stream<Result> delete(
        @Name(value = "graphName") String graphName,
        @Name(value = "relationshipType") String relationshipType
    ) {

        GraphStoreWithConfig graphStoreWithConfig = GraphStoreCatalog.get(getUsername(), graphName);

        DeleteRelationshipsConfig.of(graphName, relationshipType).validate(graphStoreWithConfig.graphStore());

        DeletionResult deletionResult = graphStoreWithConfig
            .graphStore()
            .deleteRelationships(RelationshipType.of(relationshipType));

        return Stream.of(new Result(
            graphName,
            relationshipType,
            filterDeletionResult(deletionResult, relationshipType, graphStoreWithConfig.config())
        ));
    }

    @Deprecated
    private DeletionResult filterDeletionResult(
        DeletionResult deletionResult,
        String relationshipType,
        GraphCreateConfig config
    ) {
        // We have to post-filter to hide the fact that we delete properties for other relationship projections
        RelationshipType relType = RelationshipType.of(relationshipType);
        Map<RelationshipType, RelationshipProjection> projectedRels = config
            .relationshipProjections()
            .projections();
        // we only have to post-filter when an originally projected rel-type is being removed
        if (projectedRels.containsKey(relType)) {
            Set<String> declaredProperties = projectedRels
                .get(relType).properties().mappings()
                .stream().map(PropertyMapping::propertyKey).collect(Collectors.toSet());

            return DeletionResult.of(builder -> {
                builder.deletedRelationships(deletionResult.deletedRelationships());
                deletionResult.deletedProperties()
                    .entrySet()
                    .stream()
                    .filter(entry -> declaredProperties.contains(entry.getKey()))
                    .forEach(builder::putDeletedProperty);
            });
        }
        return deletionResult;
    }

    public static class Result {
        public final String graphName;
        public final String relationshipType;

        public final long deletedRelationships;
        public final Map<String, Long> deletedProperties;

        Result(String graphName, String relationshipType, DeletionResult deletionResult) {
            this.graphName = graphName;
            this.relationshipType = relationshipType;
            this.deletedRelationships = deletionResult.deletedRelationships();
            this.deletedProperties = deletionResult.deletedProperties();
        }
    }
}
