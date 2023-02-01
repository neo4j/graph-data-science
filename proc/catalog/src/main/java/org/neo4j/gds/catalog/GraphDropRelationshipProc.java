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
package org.neo4j.gds.catalog;

import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.config.DeleteRelationshipsConfig;
import org.neo4j.gds.core.loading.DeletionResult;
import org.neo4j.gds.core.loading.GraphStoreWithConfig;
import org.neo4j.gds.core.utils.progress.JobId;
import org.neo4j.gds.core.utils.progress.tasks.TaskProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.executor.ProcPreconditions;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;

public class GraphDropRelationshipProc extends CatalogProc {

    private static final String DESCRIPTION = "Delete the relationship type for a given graph stored in the graph-catalog.";

    @Procedure(name = "gds.graph.relationships.drop", mode = READ)
    @Description(DESCRIPTION)
    public Stream<Result> dropRelationships(
        @Name(value = "graphName") String graphName,
        @Name(value = "relationshipType") String relationshipType
    ) {
        return dropRelationships(graphName, relationshipType, Optional.empty());
    }

    @Procedure(name = "gds.graph.deleteRelationships", mode = READ, deprecatedBy = "gds.graph.relationships.drop")
    @Description(DESCRIPTION)
    public Stream<Result> delete(
        @Name(value = "graphName") String graphName,
        @Name(value = "relationshipType") String relationshipType
    ) {
        var deprecationWarning = "This procedures is deprecated for removal. Please use `gds.graph.relationships.drop`";
        return dropRelationships(graphName, relationshipType, Optional.of(deprecationWarning));
    }

    private Stream<Result> dropRelationships(
        String graphName,
        String relationshipType,
        Optional<String> deprecationWarning
    ) {
        ProcPreconditions.check();

        GraphStoreWithConfig graphStoreWithConfig = graphStoreFromCatalog(graphName);

        DeleteRelationshipsConfig.of(graphName, relationshipType).validate(graphStoreWithConfig.graphStore());

        var task = Tasks.leaf("Graph :: Relationships :: Drop", 1);
        var progressTracker = new TaskProgressTracker(
            task,
            log,
            1,
            new JobId(),
            executionContext().taskRegistryFactory(),
            executionContext().userLogRegistryFactory()
        );

        deprecationWarning.ifPresent(progressTracker::logWarning);

        progressTracker.beginSubTask();
        DeletionResult deletionResult = graphStoreWithConfig
            .graphStore()
            .deleteRelationships(RelationshipType.of(relationshipType));
        progressTracker.endSubTask();

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
