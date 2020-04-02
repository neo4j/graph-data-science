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

import org.jetbrains.annotations.Nullable;
import org.neo4j.graphalgo.config.GraphWriteRelationshipConfig;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.loading.GraphStore;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.write.RelationshipExporter;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static java.util.Optional.ofNullable;
import static org.apache.commons.lang3.StringUtils.trimToNull;
import static org.neo4j.procedure.Mode.WRITE;

public class GraphWriteRelationshipProc extends CatalogProc {

    @Procedure(name = "gds.graph.writeRelationship", mode = WRITE)
    @Description("Writes the given relationship and an optional relationship property to an online Neo4j database.")
    public Stream<Result> run(
        @Name(value = "graphName") String graphName,
        @Name(value = "relationshipType") String relationshipType,
        @Name(value = "relationshipProperty", defaultValue = "") @Nullable String relationshipProperty,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        validateGraphName(graphName);
        // input
        CypherMapWrapper cypherConfig = CypherMapWrapper.create(configuration);
        Optional<String> maybeRelationshipProperty = ofNullable(trimToNull(relationshipProperty));

        GraphWriteRelationshipConfig config = GraphWriteRelationshipConfig.of(
            getUsername(),
            graphName,
            relationshipType,
            maybeRelationshipProperty,
            cypherConfig
        );
        // validation
        validateConfig(cypherConfig, config);
        GraphStore graphStore = GraphStoreCatalog.get(getUsername(), graphName).graphStore();
        config.validate(graphStore);
        // writing
        Result.Builder builder = new Result.Builder(graphName, relationshipType, maybeRelationshipProperty);
        try (ProgressTimer ignored = ProgressTimer.start(builder::withWriteMillis)) {
            long relationshipsWritten = runWithExceptionLogging(
                "Writing relationships failed",
                () -> writeRelationshipType(graphStore, config)
            );
            builder.withRelationshipsWritten(relationshipsWritten);
        }
        // result
        return Stream.of(builder.build());
    }

    private long writeRelationshipType(GraphStore graphStore, GraphWriteRelationshipConfig config) {
        RelationshipExporter exporter = RelationshipExporter
            .of(
                api,
                graphStore.getGraph(config.relationshipType(), config.relationshipProperty()),
                TerminationFlag.wrap(transaction)
            )
            .withLog(log)
            .parallel(Pools.DEFAULT, config.writeConcurrency())
            .build();
        exporter.write(config.relationshipType(), config.relationshipProperty());

        return graphStore.relationshipCount(config.relationshipType());
    }

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
