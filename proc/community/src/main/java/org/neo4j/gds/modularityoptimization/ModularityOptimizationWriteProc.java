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
package org.neo4j.gds.modularityoptimization;

import org.neo4j.gds.procedures.GraphDataScienceProcedures;
import org.neo4j.gds.procedures.community.modularityoptimization.ModularityOptimizationWriteResult;
import org.neo4j.gds.applications.algorithms.machinery.MemoryEstimateResult;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Internal;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.modularityoptimization.ModularityOptimizationSpecificationHelper.MODULARITY_OPTIMIZATION_DESCRIPTION;
import static org.neo4j.gds.procedures.ProcedureConstants.MEMORY_ESTIMATION_DESCRIPTION;
import static org.neo4j.procedure.Mode.READ;
import static org.neo4j.procedure.Mode.WRITE;

public class ModularityOptimizationWriteProc {
    @Context
    public GraphDataScienceProcedures facade;

    @Procedure(name = "gds.modularityOptimization.write", mode = WRITE)
    @Description(MODULARITY_OPTIMIZATION_DESCRIPTION)
    public Stream<ModularityOptimizationWriteResult> write(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return facade.community().modularityOptimizationWrite(graphName, configuration);
    }

    @Procedure(value = "gds.modularityOptimization.write.estimate", mode = READ)
    @Description(MEMORY_ESTIMATION_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphNameOrConfiguration") Object graphNameOrConfiguration,
        @Name(value = "algoConfiguration") Map<String, Object> algoConfiguration
    ) {
        return facade.community().modularityOptimizationEstimateWrite(graphNameOrConfiguration, algoConfiguration);
    }

    @Deprecated(forRemoval = true)
    @Internal
    @Procedure(name = "gds.beta.modularityOptimization.write", mode = WRITE, deprecatedBy = "gds.modularityOptimization.write")
    @Description(MODULARITY_OPTIMIZATION_DESCRIPTION)
    public Stream<ModularityOptimizationWriteResult> writeBeta(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        facade.deprecatedProcedures().called("gds.beta.modularityOptimization.write");
        facade
            .log()
            .warn("Procedure `gds.beta.modularityOptimization.write` has been deprecated, please use `gds.modularityOptimization.write`.");

        return write(graphName, configuration);
    }

    @Deprecated(forRemoval = true)
    @Internal
    @Procedure(value = "gds.beta.modularityOptimization.write.estimate", mode = READ, deprecatedBy = "gds.modularityOptimization.write.estimate")
    @Description(MEMORY_ESTIMATION_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimateBeta(
        @Name(value = "graphNameOrConfiguration") Object graphNameOrConfiguration,
        @Name(value = "algoConfiguration") Map<String, Object> algoConfiguration
    ) {
        facade.deprecatedProcedures().called("gds.beta.modularityOptimization.write.estimate");
        facade
            .log()
            .warn("Procedure `gds.beta.modularityOptimization.write.estimate` has been deprecated, please use `gds.modularityOptimization.write.estimate`.");

        return estimate(graphNameOrConfiguration, algoConfiguration);
    }

}
