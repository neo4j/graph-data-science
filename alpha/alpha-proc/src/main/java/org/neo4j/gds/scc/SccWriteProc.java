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
package org.neo4j.gds.scc;

import org.neo4j.gds.BaseProc;
import org.neo4j.gds.core.write.NodePropertyExporter;
import org.neo4j.gds.core.write.NodePropertyExporterBuilder;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.ProcedureExecutor;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.impl.scc.Scc.SCC_DESCRIPTION;
import static org.neo4j.procedure.Mode.WRITE;

public class SccWriteProc extends BaseProc {


    @Context
    public NodePropertyExporterBuilder<? extends NodePropertyExporter> nodePropertyExporterBuilder;

    @Procedure(value = "gds.alpha.scc.write", mode = WRITE)
    @Description(SCC_DESCRIPTION)
    public Stream<WriteResult> write(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return new ProcedureExecutor<>(
            new SccWriteSpec(),
            executionContext()
        ).compute(graphName, configuration);
    }

    @Override
    public ExecutionContext executionContext() {
        return super.executionContext().withNodePropertyExporterBuilder(nodePropertyExporterBuilder);
    }

}
