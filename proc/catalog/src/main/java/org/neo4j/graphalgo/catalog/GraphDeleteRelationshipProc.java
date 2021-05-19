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
package org.neo4j.graphalgo.catalog;

import org.neo4j.graphalgo.RelationshipType;
import org.neo4j.graphalgo.config.DeleteRelationshipsConfig;
import org.neo4j.graphalgo.core.loading.DeletionResult;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;
import org.neo4j.graphalgo.core.loading.GraphStoreWithConfig;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
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
        GraphStoreWithConfig graphStoreWithConfig = graphStoreFromCatalog(graphName);

        DeleteRelationshipsConfig.of(graphName, relationshipType).validate(graphStoreWithConfig.graphStore());

        DeletionResult deletionResult = graphStoreWithConfig
            .graphStore()
            .deleteRelationships(RelationshipType.of(relationshipType));

        return Stream.of(new Result(
            graphName,
            relationshipType,
            deletionResult
        ));
    }

    @SuppressWarnings("unused")
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
