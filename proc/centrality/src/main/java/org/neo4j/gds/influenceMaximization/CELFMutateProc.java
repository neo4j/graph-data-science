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

import org.neo4j.gds.procedures.GraphDataScience;
import org.neo4j.gds.procedures.centrality.celf.CELFMutateResult;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Internal;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.BaseProc.ESTIMATE_DESCRIPTION;
import static org.neo4j.gds.influenceMaximization.CELFStreamProc.DESCRIPTION;
import static org.neo4j.procedure.Mode.READ;

public class CELFMutateProc {

    @Context
    public GraphDataScience facade;
    @Procedure(name = "gds.influenceMaximization.celf.mutate", mode = READ)
    @Description(DESCRIPTION)
    public Stream<CELFMutateResult> mutate(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return facade.centrality().celfMutate(graphName, configuration);
    }

    @Procedure(name = "gds.influenceMaximization.celf.mutate.estimate", mode = READ)
    @Description(ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphNameOrConfiguration") Object graphNameOrConfiguration,
        @Name(value = "algoConfiguration") Map<String, Object> algoConfiguration
    ) {
        return facade.centrality().celfMutateEstimate(graphNameOrConfiguration, algoConfiguration);
    }

    @Procedure(
        name = "gds.beta.influenceMaximization.celf.mutate",
        mode = READ,
        deprecatedBy = "gds.influenceMaximization.celf.mutate"
    )
    @Description(DESCRIPTION)
    @Internal
    @Deprecated
    public Stream<CELFMutateResult> betaMutate(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        facade
            .deprecatedProcedures().called("gds.beta.influenceMaximization.celf.mutate");

        facade
            .log()
            .warn(
                "Procedure `gds.beta.influenceMaximization.celf.mutate has been deprecated, please use `gds.influenceMaximization.celf.mutate`.");
        return mutate(graphName, configuration);
    }

    @Procedure(
        name = "gds.beta.influenceMaximization.celf.mutate.estimate",
        mode = READ,
        deprecatedBy = "gds.influenceMaximization.celf.mutate.estimate"
    )
    @Description(ESTIMATE_DESCRIPTION)
    @Internal
    @Deprecated
    public Stream<MemoryEstimateResult> betaEstimate(
        @Name(value = "graphNameOrConfiguration") Object graphNameOrConfiguration,
        @Name(value = "algoConfiguration") Map<String, Object> algoConfiguration
    ) {
        facade
            .deprecatedProcedures().called("gds.beta.influenceMaximization.celf.mutate.estimate");

        facade
            .log()
            .warn(
                "Procedure `gds.beta.influenceMaximization.celf.mutate.estimate has been deprecated, please use `gds.influenceMaximization.celf.mutate.estimate`.");
        return estimate(graphNameOrConfiguration, algoConfiguration);
    }
}
