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

import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.config.GraphWriteRelationshipConfig;
import org.neo4j.gds.config.WriteConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.TaskProgressTracker;
import org.neo4j.gds.core.write.RelationshipExporter;
import org.neo4j.gds.core.write.RelationshipExporterBuilder;
import org.neo4j.gds.executor.ProcPreconditions;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.values.storable.Values;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static java.util.Optional.ofNullable;
import static org.apache.commons.lang3.StringUtils.trimToNull;
import static org.neo4j.procedure.Mode.WRITE;

public class GraphWriteRelationshipProc extends CatalogProc {

    @Context
    public RelationshipExporterBuilder<? extends RelationshipExporter> relationshipExporterBuilder;

    @Procedure(name = "gds.graph.relationship.write", mode = WRITE)
    @Description("Writes the given relationship and an optional relationship property to an online Neo4j database.")
    public Stream<Result> writeRelationships(
        @Name(value = "graphName") String graphName,
        @Name(value = "relationshipType") String relationshipType,
        @Name(value = "relationshipProperty", defaultValue = "") @Nullable String relationshipProperty,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return writeRelationships(graphName, relationshipType, relationshipProperty, configuration, Optional.empty());
    }

    @Procedure(name = "gds.graph.writeRelationship", mode = WRITE, deprecatedBy = "gds.graph.relationships.write")
    @Description("Writes the given relationship and an optional relationship property to an online Neo4j database.")
    public Stream<Result> run(
        @Name(value = "graphName") String graphName,
        @Name(value = "relationshipType") String relationshipType,
        @Name(value = "relationshipProperty", defaultValue = "") @Nullable String relationshipProperty,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        var deprecationWarning = "This procedures is deprecated for removal. Please use `gds.graph.relationship.write`";
        return writeRelationships(graphName, relationshipType, relationshipProperty, configuration, Optional.of(deprecationWarning));
    }

    private Stream<Result> writeRelationships(
        String graphName,
        String relationshipTypeString,
        String relationshipProperty,
        Map<String, Object> configuration,
        Optional<String> deprecationWarning
    ) {
        ProcPreconditions.check();
        validateGraphName(graphName);

        // input
        var cypherConfig = CypherMapWrapper.create(configuration);
        var maybeRelationshipProperty = ofNullable(trimToNull(relationshipProperty));

        var config = GraphWriteRelationshipConfig.of(
            relationshipTypeString,
            maybeRelationshipProperty,
            cypherConfig
        );

        // validation
        validateConfig(cypherConfig, config);
        var graphStore = graphStoreFromCatalog(graphName, config).graphStore();
        config.validate(graphStore);

        var relationshipType = RelationshipType.of(relationshipTypeString);
        var relationshipCount = graphStore.relationshipCount(relationshipType);

        var progressTracker = new TaskProgressTracker(
            RelationshipExporter.baseTask("Graph", relationshipCount),
            executionContext().log(),
            RelationshipExporterBuilder.DEFAULT_WRITE_CONCURRENCY,
            config.jobId(),
            executionContext().taskRegistryFactory(),
            executionContext().userLogRegistryFactory()
        );

        deprecationWarning.ifPresent(progressTracker::logWarning);

        // writing
        var builder = new Result.Builder(graphName, relationshipTypeString, maybeRelationshipProperty);
        try (var ignored = ProgressTimer.start(builder::withWriteMillis)) {
            long relationshipsWritten = runWithExceptionLogging(
                "Writing relationships failed",
                () -> writeRelationshipType(
                    graphStore,
                    config.relationshipProperty(),
                    relationshipType,
                    config.arrowConnectionInfo(),
                    progressTracker
                )
            );
            builder.withRelationshipsWritten(relationshipsWritten);
        }

        // result
        return Stream.of(builder.build());
    }

    private long writeRelationshipType(
        GraphStore graphStore,
        Optional<String> relationshipProperty,
        RelationshipType relationshipType,
        Optional<WriteConfig.ArrowConnectionInfo> arrowConnectionInfo,
        ProgressTracker progressTracker
    ) {
        var graph = graphStore.getGraph(relationshipType, relationshipProperty);

        var builder = relationshipExporterBuilder
            .withIdMappingOperator(graph::toOriginalNodeId)
            .withGraph(graph)
            .withTerminationFlag(TerminationFlag.wrap(executionContext().terminationMonitor()))
            .withArrowConnectionInfo(arrowConnectionInfo)
            .withProgressTracker(progressTracker);

        if (relationshipProperty.isPresent()) {
            var propertyKey = relationshipProperty.get();
            var propertyType = graphStore.relationshipPropertyType(propertyKey);
            if (propertyType == ValueType.LONG) {
                builder.withRelationPropertyTranslator(property -> Values.longValue((long) property));
            } else if (propertyType == ValueType.DOUBLE) {
                builder.withRelationPropertyTranslator(Values::doubleValue);
            } else {
                throw new UnsupportedOperationException("Writing non-numeric data is not supported.");
            }
            builder.build().write(relationshipType.name, propertyKey);
        } else {
            builder.build().write(relationshipType.name);
        }

        return graphStore.relationshipCount(relationshipType);
    }

    @SuppressWarnings("unused")
    public static class Result {
        public final long writeMillis;
        public final String graphName;
        public final String relationshipType;
        public final String relationshipProperty;
        public final long relationshipsWritten;
        public final long propertiesWritten;

        Result(
            long writeMillis,
            String graphName,
            String relationshipType,
            Optional<String> relationshipProperty,
            long relationshipsWritten
        ) {
            this.writeMillis = writeMillis;
            this.graphName = graphName;
            this.relationshipType = relationshipType;
            this.relationshipProperty = relationshipProperty.orElse(null);
            this.relationshipsWritten = relationshipsWritten;
            this.propertiesWritten = relationshipProperty.isPresent() ? relationshipsWritten : 0L;
        }

        static class Builder {
            private final String graphName;
            private final String relationshipType;
            private final Optional<String> maybeRelationshipProperty;

            private long writeMillis;
            private long relationshipsWritten;

            Builder withWriteMillis(long writeMillis) {
                this.writeMillis = writeMillis;
                return this;
            }

            Builder withRelationshipsWritten(long relationshipsWritten) {
                this.relationshipsWritten = relationshipsWritten;
                return this;
            }

            Builder(String graphName, String relationshipType, Optional<String> maybeRelationshipProperty) {
                this.graphName = graphName;
                this.relationshipType = relationshipType;
                this.maybeRelationshipProperty = maybeRelationshipProperty;
            }

            Result build() {
                return new Result(
                    writeMillis,
                    graphName,
                    relationshipType,
                    maybeRelationshipProperty,
                    relationshipsWritten
                );
            }
        }
    }

}
