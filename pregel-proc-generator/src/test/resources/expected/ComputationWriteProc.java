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
package org.neo4j.gds.pregel.cc;

import java.util.Map;
import java.util.stream.Stream;
import javax.annotation.processing.Generated;
import org.neo4j.gds.BaseProc;
import org.neo4j.gds.applications.algorithms.machinery.MemoryEstimateResult;
import org.neo4j.gds.core.write.NodePropertyExporterBuilder;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.MemoryEstimationExecutor;
import org.neo4j.gds.executor.ProcedureExecutor;
import org.neo4j.gds.pregel.proc.PregelWriteResult;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

@Generated("org.neo4j.gds.pregel.PregelProcessor")
public final class ComputationWriteProc extends BaseProc {

    @Context
    public NodePropertyExporterBuilder nodePropertyExporterBuilder;

    @Override
    public ExecutionContext executionContext() {
        return super.executionContext().withNodePropertyExporterBuilder(nodePropertyExporterBuilder);
    }

    @Procedure(
        name = "gds.pregel.test.write",
        mode = Mode.WRITE
    )
    @Description("Test computation description")
    public Stream<PregelWriteResult> write(@Name("graphName") String graphName,
                                           @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) {
        var specification = new ComputationWriteSpecification();
        var executor = new ProcedureExecutor<>(specification, executionContext());
        return executor.compute(graphName, configuration);
    }

    @Procedure(
        name = "gds.pregel.test.write.estimate",
        mode = Mode.READ
    )
    @Description(BaseProc.ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name("graphNameOrConfiguration") Object graphNameOrConfiguration,
        @Name("algoConfiguration") Map<String, Object> algoConfiguration) {
        var specification = new ComputationWriteSpecification();
        var executor = new MemoryEstimationExecutor<>(specification, executionContext(), transactionContext());
        return executor.computeEstimate(graphNameOrConfiguration, algoConfiguration);
    }
}
