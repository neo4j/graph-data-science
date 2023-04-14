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
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.write.RelationshipPropertiesExporterBuilder;
import org.neo4j.gds.executor.ProcPreconditions;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.values.storable.Values;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;
import static org.neo4j.gds.utils.StringJoining.join;
import static org.neo4j.procedure.Mode.WRITE;

public class GraphWriteRelationshipPropertiesProc extends CatalogProc {

    @Context
    public RelationshipPropertiesExporterBuilder exporterBuilder;

    @Procedure(name = "gds.graph.relationshipProperties.write", mode = WRITE)
    @Description("Writes the given relationship and a list of relationship properties to an online Neo4j database.")
    public Stream<Result> writeRelationships(
        @Name(value = "graphName") String graphName,
        @Name(value = "relationshipType") String relationshipType,
        @Name(value = "relationshipProperties") List<String> relationshipProperties
    ) {

        ProcPreconditions.check();
        validateGraphName(graphName);

        // validation
        var graphStore = graphStoreFromCatalog(graphName).graphStore();
        validate(graphStore, relationshipType, relationshipProperties);

        var relationshipCount = graphStore.relationshipCount(RelationshipType.of(relationshipType));

        var exporter = exporterBuilder
            .withGraphStore(graphStore)
            .withRelationPropertyTranslator(Values::doubleValue)
            .withTerminationFlag(TerminationFlag.wrap(executionContext().terminationMonitor()))
            .withProgressTracker(ProgressTracker.NULL_TRACKER)
            .build();

        // writing
        var resultBuilder = new Result.Builder(graphName, relationshipType, relationshipProperties);
        try (var ignored = ProgressTimer.start(resultBuilder::withWriteMillis)) {
            runWithExceptionLogging(
                "Writing relationships failed",
                () -> exporter.write(relationshipType, relationshipProperties)
            );
            resultBuilder.withRelationshipsWritten(relationshipCount);
        }

        return Stream.of(resultBuilder.build());
    }

    private void validate(GraphStore graphStore, String relationshipType, Collection<String> relationshipProperties) {
        if (!graphStore.hasRelationshipType(RelationshipType.of(relationshipType))) {
            throw new IllegalArgumentException(formatWithLocale(
                "Relationship type `%s` not found. Available types: %s",
                relationshipType,
                join(graphStore.relationshipTypes().stream().map(RelationshipType::name).collect(Collectors.toSet()))
            ));
        }

        Set<String> availableProperties = graphStore.relationshipPropertyKeys(RelationshipType.of(relationshipType));

        var propertiesList = relationshipProperties
            .stream()
            .filter(relProperty -> !availableProperties.contains(relProperty))
            .collect(
                Collectors.toList());

        if (!propertiesList.isEmpty()) {
            throw new IllegalArgumentException(formatWithLocale(
                "Some  properties are missing %s for relationship type '%s'. Available properties: %s",
                join(propertiesList),
                relationshipType,
                join(availableProperties)
            ));
        }
    }

    @SuppressWarnings("unused")
    public static class Result {
        public final long writeMillis;
        public final String graphName;
        public final String relationshipType;
        public final List<String> relationshipProperties;
        public final long relationshipsWritten;
        public final long propertiesWritten;

        Result(
            long writeMillis,
            String graphName,
            String relationshipType,
            List<String> relationshipProperties,
            long relationshipsWritten
        ) {
            this.writeMillis = writeMillis;
            this.graphName = graphName;
            this.relationshipType = relationshipType;
            this.relationshipProperties = relationshipProperties;
            this.relationshipsWritten = relationshipsWritten;
            this.propertiesWritten = relationshipsWritten * relationshipProperties.size();
        }

        static class Builder {
            private final String graphName;
            private final String relationshipType;
            private final List<String> relationProperties;

            private long writeMillis;
            private long relationshipsWritten;

            Builder(String graphName, String relationshipType, List<String> relationProperties) {
                this.graphName = graphName;
                this.relationshipType = relationshipType;
                this.relationProperties = relationProperties;
            }

            Builder withWriteMillis(long writeMillis) {
                this.writeMillis = writeMillis;
                return this;
            }

            Builder withRelationshipsWritten(long relationshipsWritten) {
                this.relationshipsWritten = relationshipsWritten;
                return this;
            }

            Result build() {
                return new Result(
                    writeMillis,
                    graphName,
                    relationshipType,
                    relationProperties,
                    relationshipsWritten
                );
            }
        }
    }

}
