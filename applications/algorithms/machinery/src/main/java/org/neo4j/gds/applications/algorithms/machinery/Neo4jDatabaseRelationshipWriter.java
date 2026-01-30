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
package org.neo4j.gds.applications.algorithms.machinery;

import org.neo4j.gds.api.ExportedRelationship;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.api.ResultStore;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.api.properties.relationships.RelationshipWithPropertyConsumer;
import org.neo4j.gds.applications.algorithms.metadata.RelationshipsWritten;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.utils.logging.LoggerForProgressTrackingAdapter;
import org.neo4j.gds.core.JobId;
import org.neo4j.gds.core.utils.progress.tasks.TaskProgressTracker;
import org.neo4j.gds.core.write.RelationshipExporter;
import org.neo4j.gds.core.write.RelationshipExporterBuilder;
import org.neo4j.gds.core.write.RelationshipStreamExporter;
import org.neo4j.gds.core.write.RelationshipStreamExporterBuilder;
import org.neo4j.gds.logging.Log;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

final class Neo4jDatabaseRelationshipWriter {

    static RelationshipsWritten writeRelationshipsFromGraph(
        String writeRelationshipType,
        String writeProperty,
        RequestScopedDependencies requestScopedDependencies,
        RelationshipExporterBuilder relationshipExporterBuilder,
        Graph graph,
        IdMap rootIdMap,
        Log log,
        String taskName,
        Optional<ResultStore> resultStore,
        RelationshipWithPropertyConsumer relationshipConsumer,
        JobId jobId
    ) {

        var progressTracker = TaskProgressTracker.create(
            RelationshipExporter.baseTask(taskName, graph.relationshipCount()),
            new LoggerForProgressTrackingAdapter(log),
            RelationshipExporterBuilder.TYPED_DEFAULT_WRITE_CONCURRENCY,
            requestScopedDependencies.correlationId(),
            requestScopedDependencies.taskRegistryFactory()
        );

        var exporter = relationshipExporterBuilder
            .withIdMappingOperator(rootIdMap::toOriginalNodeId)
            .withGraph(graph)
            .withTerminationFlag(requestScopedDependencies.terminationFlag())
            .withProgressTracker(progressTracker)
            .withResultStore(resultStore)
            .withJobId(jobId)
            .build();

        try {
            exporter.write(
                writeRelationshipType,
                writeProperty,
                relationshipConsumer
            );
        } catch (Exception e) {
            progressTracker.endSubTaskWithFailure();
            throw e;
        } finally {
            progressTracker.release();
        }

        return new RelationshipsWritten(graph.relationshipCount());
    }

    static RelationshipsWritten writeRelationshipsFromStream(
        String writeRelationshipType,
        List<String> properties,
        List<ValueType> valueTypes,
        RequestScopedDependencies requestScopedDependencies,
        RelationshipStreamExporterBuilder relationshipStreamExporterBuilder,
        Stream<ExportedRelationship> relationshipStream,
        IdMap rootIdMap,
        Log log,
        String taskName,
        Optional<ResultStore> maybeResultStore,
        JobId jobId
    ) {

        var progressTracker = TaskProgressTracker.create(
            RelationshipStreamExporter.baseTask(taskName),
            new LoggerForProgressTrackingAdapter(log),
            new Concurrency(1),
            requestScopedDependencies.correlationId(),
            requestScopedDependencies.taskRegistryFactory()
        );

        // When we are writing to the result store, the result stream might not be consumed
        // inside the current transaction. This causes the stream to immediately return an empty stream
        // as the termination flag, which is bound to the current transaction is set to true. We therefore
        // need to collect the stream and trigger an eager computation.
        var maybeCollectedStream = maybeResultStore
            .map(__ -> relationshipStream.toList().stream())
            .orElse(relationshipStream);

        // configure the exporter
        var relationshipStreamExporter = relationshipStreamExporterBuilder
            .withResultStore(maybeResultStore)
            .withIdMappingOperator(rootIdMap::toOriginalNodeId)
            .withProgressTracker(progressTracker)
            .withRelationships(maybeCollectedStream)
            .withTerminationFlag(requestScopedDependencies.terminationFlag())
            .withJobId(jobId)
            .build();

        var relationshipsWritten = relationshipStreamExporter.write(
            writeRelationshipType,
            properties,
            valueTypes
        );

        return new RelationshipsWritten(relationshipsWritten);
    }


    private Neo4jDatabaseRelationshipWriter() {}
}
