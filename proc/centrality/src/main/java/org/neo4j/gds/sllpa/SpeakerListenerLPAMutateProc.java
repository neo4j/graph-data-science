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
package org.neo4j.gds.sllpa;

import org.neo4j.gds.applications.algorithms.machinery.MemoryEstimateResult;
import org.neo4j.gds.procedures.GraphDataScienceProcedures;
import org.neo4j.gds.procedures.algorithms.centrality.SpeakerListenerLPAMutateResult;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Internal;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.procedures.ProcedureConstants.MEMORY_ESTIMATION_DESCRIPTION;
import static org.neo4j.gds.sllpa.Constants.SLLP_DESCRIPTION;
import static org.neo4j.procedure.Mode.READ;

public class SpeakerListenerLPAMutateProc {

    @Context
    public GraphDataScienceProcedures facade;

    @Procedure(value = "gds.sllpa.mutate", mode = READ)
    @Description(SLLP_DESCRIPTION)
    public Stream<SpeakerListenerLPAMutateResult> mutate(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return facade.algorithms().centrality().speakerListenerLPAMutateStub().execute(graphName,configuration);
    }

    @Procedure(value = "gds.sllpa.mutate.estimate", mode = READ)
    @Description(MEMORY_ESTIMATION_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphNameOrConfiguration") Object graphNameOrConfiguration,
        @Name(value = "algoConfiguration") Map<String, Object> algoConfiguration
    ) {
        return facade.algorithms().centrality().speakerListenerLPAMutateStub().estimate(graphNameOrConfiguration, algoConfiguration);
    }

    @Internal
    @Deprecated(forRemoval = true)
    @Procedure(name = "gds.alpha.sllpa.mutate", mode = READ, deprecatedBy = "gds.sllpa.mutate")
    @Description(SLLP_DESCRIPTION)
    public Stream<SpeakerListenerLPAMutateResult> alphaMutate(
        @Name("graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ){
        facade.deprecatedProcedures().called("gds.alpha.sllpa.mutate");
        return  mutate(graphName,configuration);
    }

    @Internal
    @Deprecated(forRemoval = true)
    @Procedure(name = "gds.alpha.sllpa.mutate.estimate", mode = Mode.READ, deprecatedBy = "gds.sllpa.mutate.estimate")
    @Description(MEMORY_ESTIMATION_DESCRIPTION)
    public Stream<MemoryEstimateResult> alphaEstimate(
        @Name("graphNameOrConfiguration") Object graphNameOrConfiguration,
        @Name("algoConfiguration") Map<String, Object> algoConfiguration) {
        facade.deprecatedProcedures().called("gds.alpha.sllpa.mutate.estimate");
        return  estimate(graphNameOrConfiguration,algoConfiguration);
    }

}
