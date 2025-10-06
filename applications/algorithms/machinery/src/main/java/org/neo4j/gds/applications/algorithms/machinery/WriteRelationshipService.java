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
import org.neo4j.gds.core.JobId;
import org.neo4j.gds.logging.Log;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

public class WriteRelationshipService {
    private final Log log;
    private final RequestScopedDependencies requestScopedDependencies;
    private final WriteContext writeContext;

    public WriteRelationshipService(Log log, RequestScopedDependencies requestScopedDependencies, WriteContext writeContext) {
        this.log = log;
        this.requestScopedDependencies = requestScopedDependencies;
        this.writeContext = writeContext;
    }

    public RelationshipsWritten writeFromGraph(
        String writeRelationshipType,
        String writeProperty,
        Graph writeGraph,
        IdMap rootIdMap,
        String taskName,
        Optional<ResultStore> resultStore,
        RelationshipWithPropertyConsumer relationshipWithPropertyConsumer,
        JobId jobId
    ) {

        return Neo4jDatabaseRelationshipWriter.writeRelationshipsFromGraph(
            writeRelationshipType,
            writeProperty,
            requestScopedDependencies.taskRegistryFactory(),
            writeContext.relationshipExporterBuilder(),
            writeGraph,
            rootIdMap,
            log,
            taskName,
            requestScopedDependencies.terminationFlag(),
            resultStore,
            relationshipWithPropertyConsumer,
            jobId
        );
    }

    public RelationshipsWritten writeFromRelationshipStream(
        String writeRelationshipType,
        List<String> properties,
        List<ValueType> valueTypes,
        Stream<ExportedRelationship> relationshipStream,
        IdMap rootIdMap,
        String taskName,
        Optional<ResultStore> resultStore,
        JobId jobId
    ){

        return Neo4jDatabaseRelationshipWriter.writeRelationshipsFromStream(
            writeRelationshipType,
            properties,
            valueTypes,
            requestScopedDependencies.taskRegistryFactory(),
            writeContext.relationshipStreamExporterBuilder(),
            relationshipStream,
            rootIdMap,
            log,
            taskName,
            requestScopedDependencies.terminationFlag(),
            resultStore,
            jobId
        );
    }


}
