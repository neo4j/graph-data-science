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
package org.neo4j.gds.approxmaxkcut;

import org.neo4j.gds.procedures.GraphDataScienceProcedures;
import org.neo4j.gds.procedures.community.approxmaxkcut.ApproxMaxKCutStreamResult;
import org.neo4j.gds.applications.algorithms.machinery.MemoryEstimateResult;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Internal;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.approxmaxkcut.ApproxMaxKCut.APPROX_MAX_K_CUT_DESCRIPTION;
import static org.neo4j.gds.procedures.ProcedureConstants.MEMORY_ESTIMATION_DESCRIPTION;
import static org.neo4j.procedure.Mode.READ;

public class ApproxMaxKCutStreamProc {
    @Context
    public GraphDataScienceProcedures facade;

    @Procedure(value = "gds.maxkcut.stream", mode = READ)
    @Description(APPROX_MAX_K_CUT_DESCRIPTION)
    public Stream<ApproxMaxKCutStreamResult> stream(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return facade.community().approxMaxKCutStream(graphName, configuration);
    }

    @Procedure(value = "gds.maxkcut.stream.estimate", mode = READ)
    @Description(MEMORY_ESTIMATION_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphNameOrConfiguration") Object graphNameOrConfiguration,
        @Name(value = "algoConfiguration") Map<String, Object> algoConfiguration
    ) {
        return facade.community().approxMaxKCutEstimateStream(graphNameOrConfiguration, algoConfiguration);
    }

    @Deprecated
    @Internal
    @Procedure(value = "gds.alpha.maxkcut.stream", mode = READ, deprecatedBy = "gds.maxcut.stream")
    @Description(APPROX_MAX_K_CUT_DESCRIPTION)
    public Stream<ApproxMaxKCutStreamResult> streamAlpha(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        facade.deprecatedProcedures().called("gds.alpha.maxkcut.stream");

        facade
            .log()
            .warn("Procedure `gds.alpha.maxkcut.stream` has been deprecated, please use `gds.maxkcut.stream`.");

        return stream(graphName, configuration);
    }

    @Deprecated
    @Internal
    @Procedure(value = "gds.alpha.maxkcut.stream.estimate", mode = READ, deprecatedBy = "gds.maxcut.stream.estimate")
    @Description(MEMORY_ESTIMATION_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimateAlpha(
        @Name(value = "graphNameOrConfiguration") Object graphNameOrConfiguration,
        @Name(value = "algoConfiguration") Map<String, Object> algoConfiguration
    ) {
        facade.deprecatedProcedures().called("gds.alpha.maxkcut.stream.estimate");
        facade
            .log()
            .warn("Procedure `gds.alpha.maxkcut.stream.estimate` has been deprecated, please use `gds.maxkcut.stream.estimate`.");
        return estimate(graphNameOrConfiguration, algoConfiguration);
    }
}
