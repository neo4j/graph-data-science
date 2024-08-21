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
package org.neo4j.gds.testproc;

import org.neo4j.gds.BaseProc;
import org.neo4j.gds.executor.ProcedureExecutor;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

public class ProcedureThatFailsDuringTask extends BaseProc {
    @Procedure(name = "very.strange.procedure", mode = Mode.READ)
    public Stream<OutputFromProcedureThatFailsDuringTask> run(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        var result = new ProcedureExecutor<>(
            new SpecForProcedureThatFailsDuringTask(),
            executionContext()
        ).compute(graphName, configuration);

        // meaningless code to avoid spotBugs error
        OutputFromProcedureThatFailsDuringTask out = new OutputFromProcedureThatFailsDuringTask();
        int i = out.out.hashCode();
        return i * i == -1 ? result : Stream.of(out);
    }
}
