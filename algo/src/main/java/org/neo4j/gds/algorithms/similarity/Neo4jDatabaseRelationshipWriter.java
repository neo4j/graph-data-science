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
package org.neo4j.gds.algorithms.similarity;

import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.api.RelationshipWithPropertyConsumer;
import org.neo4j.gds.api.ResultStore;
import org.neo4j.gds.config.ArrowConnectionInfo;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.tasks.TaskProgressTracker;
import org.neo4j.gds.core.write.RelationshipExporter;
import org.neo4j.gds.core.write.RelationshipExporterBuilder;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

public final class Neo4jDatabaseRelationshipWriter {
    public static WriteRelationshipResult writeRelationship(
        String writeRelationshipType,
        String writeProperty,
        TaskRegistryFactory taskRegistryFactory,
        RelationshipExporterBuilder relationshipExporterBuilder,
        Graph graph,
        GraphStore graphStore,
        IdMap rootIdMap,
        Log log,
        String taskName,
        TerminationFlag algorithmTerminationFlag,
        Concurrency concurrency,
        Optional<ArrowConnectionInfo> arrowConnectionInfo,
        Optional<ResultStore> resultStore,
        RelationshipWithPropertyConsumer relationshipConsumer

    ){
        var writeMillis = new AtomicLong();
        try (ProgressTimer ignored = ProgressTimer.start(writeMillis::set)) {
            var progressTracker = new TaskProgressTracker(
                RelationshipExporter.baseTask(taskName, graph.relationshipCount()),
                (org.neo4j.logging.Log) log.getNeo4jLog(),
                RelationshipExporterBuilder.TYPED_DEFAULT_WRITE_CONCURRENCY,
                taskRegistryFactory
            );

            var exporter = relationshipExporterBuilder
                .withIdMappingOperator(rootIdMap::toOriginalNodeId)
                .withGraph(graph)
                .withTerminationFlag(algorithmTerminationFlag)
                .withProgressTracker(progressTracker)
                .withConcurrency(concurrency)
                .withArrowConnectionInfo(
                    arrowConnectionInfo,
                    graphStore.databaseInfo().remoteDatabaseId().map(DatabaseId::databaseName)
                )
                .withResultStore(resultStore)
                .build();

            exporter.write(
                writeRelationshipType,
                writeProperty,
               relationshipConsumer
            );

        }
        return new WriteRelationshipResult(graph.relationshipCount(), writeMillis.get());
    }

    private Neo4jDatabaseRelationshipWriter() {}
}
