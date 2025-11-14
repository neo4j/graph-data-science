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
package org.neo4j.gds.ml.splitting;

import org.neo4j.gds.applications.algorithms.machinery.MemoryEstimateResult;
import org.neo4j.gds.procedures.GraphDataScienceProcedures;
import org.neo4j.gds.procedures.algorithms.machinelearning.SplitRelationshipsMutateResult;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Internal;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.ml.splitting.Constants.DESCRIPTION;
import static org.neo4j.gds.procedures.ProcedureConstants.MEMORY_ESTIMATION_DESCRIPTION;
import static org.neo4j.procedure.Mode.READ;

public class SplitRelationshipsMutateProc {
    @Context
    public GraphDataScienceProcedures facade;

    @Procedure(name = "gds.ml.splitRelationships.mutate", mode = READ)
    @Description(DESCRIPTION)
    public Stream<SplitRelationshipsMutateResult> mutate(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return facade.algorithms().machineLearning().splitRelationshipsMutateStub().execute(graphName, configuration);
    }

    @Procedure(name = "gds.alpha.ml.splitRelationships.mutate", mode = READ, deprecatedBy = "gds.ml.splitRelationships.mutate")
    @Description(DESCRIPTION)
    @Internal
    @Deprecated
    public Stream<SplitRelationshipsMutateResult> alphaMutate(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        facade.deprecatedProcedures().called("gds.alpha.ml.splitRelationships.mutate");
        facade.log().warn("Function `gds.alpha.ml.splitRelationships.mutate` has been deprecated, please use `gds.ml.splitRelationships.mutate`.");

        return mutate(graphName, configuration);
    }

    @Procedure(value = "gds.ml.splitRelationships.mutate.estimate", mode = READ)
    @Description(MEMORY_ESTIMATION_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphNameOrConfiguration") Object graphNameOrConfiguration,
        @Name(value = "algoConfiguration") Map<String, Object> algoConfiguration
    ) {
        return facade.algorithms().machineLearning().splitRelationshipsMutateStub().estimate(
            graphNameOrConfiguration,
            algoConfiguration
        );
    }

    @Procedure(value = "gds.alpha.ml.splitRelationships.mutate.estimate", mode = READ, deprecatedBy = "gds.ml.splitRelationships.mutate.estimate")
    @Description(MEMORY_ESTIMATION_DESCRIPTION)
    @Internal
    @Deprecated
    public Stream<MemoryEstimateResult> alphaEstimate(
        @Name(value = "graphNameOrConfiguration") Object graphNameOrConfiguration,
        @Name(value = "algoConfiguration") Map<String, Object> algoConfiguration
    ) {
        facade.deprecatedProcedures().called("gds.alpha.ml.splitRelationships.mutate.estimate");
        facade.log().warn("Function `gds.alpha.ml.splitRelationships.mutate.estimate` has been deprecated, please use `gds.ml.splitRelationships.mutate.estimate`.");

        return estimate(graphNameOrConfiguration, algoConfiguration);
    }
}
