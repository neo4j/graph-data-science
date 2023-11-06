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
package org.neo4j.gds.applications.graphstorecatalog;

import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.config.WriteConfig;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.TaskProgressTracker;
import org.neo4j.gds.core.utils.warnings.UserLogRegistryFactory;
import org.neo4j.gds.core.write.RelationshipExporter;
import org.neo4j.gds.core.write.RelationshipExporterBuilder;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.termination.TerminationFlag;
import org.neo4j.values.storable.Values;

import java.util.Optional;

public class WriteRelationshipsApplication {
    private final Log log;

    public WriteRelationshipsApplication(Log log) {
        this.log = log;
    }

    WriteRelationshipResult compute(
        RelationshipExporterBuilder relationshipExporterBuilder,
        TaskRegistryFactory taskRegistryFactory,
        TerminationFlag terminationFlag,
        UserLogRegistryFactory userLogRegistryFactory,
        GraphStore graphStore,
        GraphName graphName,
        GraphWriteRelationshipConfig configuration
    ) {
        var relationshipType = RelationshipType.of(configuration.relationshipType());
        var relationshipCount = graphStore.relationshipCount(relationshipType);

        var progressTracker = new TaskProgressTracker(
            RelationshipExporter.baseTask("Graph", relationshipCount),
            (org.neo4j.logging.Log) log.getNeo4jLog(),
            RelationshipExporterBuilder.DEFAULT_WRITE_CONCURRENCY,
            configuration.jobId(),
            taskRegistryFactory,
            userLogRegistryFactory
        );

        // writing
        var builder = new WriteRelationshipResult.Builder(
            graphName.getValue(),
            configuration.relationshipType(),
            configuration.relationshipProperty()
        );

        try (var ignored = ProgressTimer.start(builder::withWriteMillis)) {
            try {
                long relationshipsWritten = writeRelationshipType(
                    relationshipExporterBuilder,
                    terminationFlag,
                    progressTracker,
                    configuration.arrowConnectionInfo(),
                    graphStore,
                    relationshipType,
                    configuration.relationshipProperty()
                );

                builder.withRelationshipsWritten(relationshipsWritten);

                // result
                return builder.build();
            } catch (RuntimeException e) {
                log.warn("Writing relationships failed", e);
                throw e;
            }
        }
    }

    private long writeRelationshipType(
        RelationshipExporterBuilder relationshipExporterBuilder,
        TerminationFlag terminationFlag,
        ProgressTracker progressTracker,
        Optional<WriteConfig.ArrowConnectionInfo> arrowConnectionInfo,
        GraphStore graphStore,
        RelationshipType relationshipType,
        Optional<String> relationshipProperty
    ) {
        var graph = graphStore.getGraph(relationshipType, relationshipProperty);

        var builder = relationshipExporterBuilder
            .withIdMappingOperator(graph::toOriginalNodeId)
            .withGraph(graph)
            .withTerminationFlag(terminationFlag)
            .withArrowConnectionInfo(arrowConnectionInfo, graphStore.databaseInfo().databaseId().databaseName())
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
}
