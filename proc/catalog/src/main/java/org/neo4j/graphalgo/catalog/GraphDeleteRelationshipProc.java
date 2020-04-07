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

import org.neo4j.graphalgo.ElementIdentifier;
import org.neo4j.graphalgo.PropertyMapping;
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

import static java.util.stream.Collectors.toMap;
import static org.neo4j.procedure.Mode.READ;

public class GraphDeleteRelationshipProc extends CatalogProc {

    private static final String DESCRIPTION = "";

    @Procedure(name = "gds.graph.deleteRelationshipType", mode = READ)
    @Description(DESCRIPTION)
    public Stream<Result> delete(
        @Name(value = "graphName") String graphName,
        @Name(value = "relationshipType") String relationshipType
    ) {
        GraphStoreWithConfig graphStoreWithConfig = GraphStoreCatalog.get(getUsername(), graphName);
        Set<String> relationshipTypes = graphStoreWithConfig.graphStore().relationshipTypes();

        if (relationshipTypes.size() == 1) {
            throw new IllegalArgumentException(String.format(
                "Deleting the last relationship type ('%s') from a graph ('%s') is not supported. " +
                "Use `gds.graph.drop()` to drop the entire graph instead.",
                relationshipType,
                graphName
            ));
        }

        if (!relationshipTypes.contains(relationshipType)) {
            throw new IllegalArgumentException(String.format(
                "No relationship type '%s' found in graph '%s'.",
                relationshipType,
                graphName
            ));
        }

        DeletionResult deletionResult = graphStoreWithConfig.graphStore().deleteRelationshipType(relationshipType);

        // We have to post-filter to hide the fact that we delete properties for other relationship projections
        Set<String> declaredProperties = graphStoreWithConfig
            .config()
            .relationshipProjections()
            .projections()
            .get(ElementIdentifier.of(relationshipType)).properties().mappings()
            .stream().map(PropertyMapping::propertyKey).collect(Collectors.toSet());

        DeletionResult filteredDeletionResult = DeletionResult.of(builder -> {
            builder
                .from(deletionResult)
                .deletedProperties(deletionResult
                    .deletedProperties()
                    .entrySet()
                    .stream()
                    .filter(entry -> declaredProperties.contains(entry.getKey()))
                    .collect(toMap(Map.Entry::getKey, Map.Entry::getValue)));
        });

        return Stream.of(new Result(graphName, relationshipType, filteredDeletionResult));
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
