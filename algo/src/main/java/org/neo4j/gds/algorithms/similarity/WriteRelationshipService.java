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
import org.neo4j.gds.api.ResultStore;
import org.neo4j.gds.applications.algorithms.machinery.ProcedureContext;
import org.neo4j.gds.applications.algorithms.machinery.RequestScopedDependencies;
import org.neo4j.gds.config.ArrowConnectionInfo;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.utils.progress.JobId;
import org.neo4j.gds.logging.Log;

import java.util.Optional;

public class WriteRelationshipService {
    private final Log log;
    private final RequestScopedDependencies requestScopedDependencies;
    private final  ProcedureContext procedureContext;
    public WriteRelationshipService(Log log, RequestScopedDependencies requestScopedDependencies, ProcedureContext procedureContext) {
        this.log = log;
        this.requestScopedDependencies = requestScopedDependencies;
        this.procedureContext  =procedureContext;
    }

    public WriteRelationshipResult write(
        String writeRelationshipType,
        String writeProperty,
        Graph graph,
        GraphStore graphStore,
        IdMap rootIdMap,
        String taskName,
        Concurrency concurrency,
        Optional<ArrowConnectionInfo> arrowConnectionInfo,
        Optional<ResultStore> resultStore,
        RelationshipWithPropertyConsumer relationshipWithPropertyConsumer,
        JobId jobId
    ) {
        return Neo4jDatabaseRelationshipWriter.writeRelationship(
            writeRelationshipType,
            writeProperty,
            requestScopedDependencies.getTaskRegistryFactory(),
            procedureContext.getRelationshipExporterBuilder(),
            graph,
            graphStore,
            rootIdMap,
            log,
            taskName,
            requestScopedDependencies.getTerminationFlag(),
            concurrency,
            arrowConnectionInfo,
            resultStore,
            relationshipWithPropertyConsumer,
            jobId
        );
    }
}
