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
package org.neo4j.gds.influenceMaximization;

import org.neo4j.gds.BaseProc;
import org.neo4j.gds.executor.MemoryEstimationExecutor;
import org.neo4j.gds.executor.ProcedureExecutor;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Internal;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;

public class CELFStreamProc extends BaseProc {

    public static final String DESCRIPTION = "The Cost Effective Lazy Forward (CELF) algorithm aims to find k nodes that maximize the expected spread of influence in the network.";

    @Procedure(name = "gds.influenceMaximization.celf.stream", mode = READ)
    @Description(DESCRIPTION)
    public Stream<StreamResult> stream(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return new ProcedureExecutor<>(
            new CELFStreamSpec(),
            executionContext()
        ).compute(graphName, configuration);
    }

    @Procedure(name = "gds.influenceMaximization.celf.stream.estimate", mode = READ)
    @Description(DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphName") Object graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        var streamSpec = new CELFStreamSpec();

        return new MemoryEstimationExecutor<>(
            streamSpec,
            executionContext(),
            transactionContext()
        ).computeEstimate(graphName, configuration);
    }

    @Procedure(
        name = "gds.beta.influenceMaximization.celf.stream",
        mode = READ,
        deprecatedBy = "gds.influenceMaximization.celf.stream"
    )
    @Internal
    @Description(DESCRIPTION)
    @Deprecated
    public Stream<StreamResult> betaStream(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        executionContext()
            .metricsFacade()
            .deprecatedProcedures().called("gds.beta.influenceMaximization.celf.stream");

        executionContext()
            .log()
            .warn(
                "Procedure `gds.beta.influenceMaximization.celf.stream has been deprecated, please use `gds.influenceMaximization.celf.stream`.");
        return stream(graphName, configuration);
    }

    @Procedure(
        name = "gds.beta.influenceMaximization.celf.stream.estimate",
        mode = READ,
        deprecatedBy = "gds.influenceMaximization.celf.stream.estimate"
    )
    @Internal
    @Description(DESCRIPTION)
    @Deprecated
    public Stream<MemoryEstimateResult> betaEstimate(
        @Name(value = "graphName") Object graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        executionContext()
            .metricsFacade()
            .deprecatedProcedures().called("gds.beta.influenceMaximization.celf.stream.estimate");

        executionContext()
            .log()
            .warn(
                "Procedure `gds.beta.influenceMaximization.celf.stream.estimate has been deprecated, please use `gds.influenceMaximization.celf.stream.estimate`.");
        return estimate(graphName, configuration);
    }

}
