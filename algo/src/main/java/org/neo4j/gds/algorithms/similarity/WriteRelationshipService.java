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

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.api.RelationshipWithPropertyConsumer;
import org.neo4j.gds.config.ArrowConnectionInfo;
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.gds.core.write.RelationshipExporterBuilder;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.Optional;

public class WriteRelationshipService {
    private final Log log;
    private final RelationshipExporterBuilder relationshipExporterBuilder;
    private final TaskRegistryFactory taskRegistryFactory;
    private final TerminationFlag terminationFlag;

    public WriteRelationshipService(
        Log log,
        RelationshipExporterBuilder relationshipExporterBuilder,
        TaskRegistryFactory taskRegistryFactory,
        TerminationFlag terminationFlag
    ) {
        this.log = log;
        this.relationshipExporterBuilder = relationshipExporterBuilder;
        this.taskRegistryFactory = taskRegistryFactory;
        this.terminationFlag = terminationFlag;
    }

    public WriteRelationshipResult write(
        String writeRelationshipType,
        String writeProperty,
        Graph graph,
        GraphStore graphStore,
        IdMap rootIdMap,
        String taskName,
        Optional<ArrowConnectionInfo> arrowConnectionInfo
    ) {
        return write(
            writeRelationshipType,
            writeProperty,
            graph,
            graphStore,
            rootIdMap,
            taskName,
            arrowConnectionInfo,
            (sourceNodeId, targetNodeId, property) -> true
        );
    }

    public WriteRelationshipResult write(
        String writeRelationshipType,
        String writeProperty,
        Graph graph,
        GraphStore graphStore,
        IdMap rootIdMap,
        String taskName,
        Optional<ArrowConnectionInfo> arrowConnectionInfo,
        RelationshipWithPropertyConsumer relationshipWithPropertyConsumer
    ) {
        return Neo4jDatabaseRelationshipWriter.writeRelationship(
            writeRelationshipType,
            writeProperty,
            taskRegistryFactory,
            relationshipExporterBuilder,
            graph,
            graphStore,
            rootIdMap,
            log,
            taskName,
            terminationFlag,
            arrowConnectionInfo,
            relationshipWithPropertyConsumer
        );
    }
}
