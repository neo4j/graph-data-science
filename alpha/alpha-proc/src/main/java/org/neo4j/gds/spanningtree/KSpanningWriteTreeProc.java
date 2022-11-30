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
package org.neo4j.gds.spanningtree;

import org.neo4j.gds.BaseProc;
import org.neo4j.gds.core.write.NodePropertyExporter;
import org.neo4j.gds.core.write.NodePropertyExporterBuilder;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.ImmutableExecutionContext;
import org.neo4j.gds.executor.ProcedureExecutor;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.WRITE;

public class KSpanningWriteTreeProc extends BaseProc {

    static final String DESCRIPTION =
        "The K-spanning tree algorithm starts from a root node and returns a spanning tree with exactly k nodes";

    @Context
    public NodePropertyExporterBuilder<? extends NodePropertyExporter> nodePropertyExporterBuilder;
    
    @Procedure(value = "gds.alpha.kSpanningTree.write", mode = WRITE)
    @Description(DESCRIPTION)
    public Stream<WriteResult> write(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return new ProcedureExecutor<>(
            new KSpanningTreeWriteSpec(),
            executionContext()
        ).compute(graphName, configuration, true, true);
    }
    @Override
    public ExecutionContext executionContext() {
        return ImmutableExecutionContext
            .builder()
            .databaseService(databaseService)
            .log(log)
            .procedureTransaction(procedureTransaction)
            .transaction(transaction)
            .callContext(callContext)
            .userLogRegistryFactory(userLogRegistryFactory)
            .taskRegistryFactory(taskRegistryFactory)
            .username(username())
            .nodePropertyExporterBuilder(nodePropertyExporterBuilder)
            .build();
    }

}

