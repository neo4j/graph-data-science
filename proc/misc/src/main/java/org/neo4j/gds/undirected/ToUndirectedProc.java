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
package org.neo4j.gds.undirected;

import org.neo4j.gds.applications.algorithms.machinery.MemoryEstimateResult;
import org.neo4j.gds.procedures.GraphDataScienceProcedures;
import org.neo4j.gds.procedures.algorithms.miscellaneous.ToUndirectedMutateResult;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Internal;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.procedures.ProcedureConstants.MEMORY_ESTIMATION_DESCRIPTION;
import static org.neo4j.gds.undirected.Constants.TO_UNDIRECTED_DESCRIPTION;
import static org.neo4j.procedure.Mode.READ;

public class ToUndirectedProc {
    @Context
    public GraphDataScienceProcedures facade;

    @Internal
    @Deprecated(forRemoval = true)
    @Procedure(
        value = "gds.beta.graph.relationships.toUndirected", mode = READ,
        deprecatedBy = Constants.CALLABLE_NAME
    )
    @Description(TO_UNDIRECTED_DESCRIPTION)
    public Stream<ToUndirectedMutateResult> mutateDeprecated(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        facade.deprecatedProcedures().called("gds.beta.graph.relationships.toUndirected");
        facade
            .log()
            .warn("Procedure `gds.beta.graph.relationships.toUndirected` has been deprecated, please use `gds.graph.relationships.toUndirected`.");
        return mutate(graphName, configuration);
    }

    @Procedure(value = "gds.graph.relationships.toUndirected", mode = READ)
    @Description(TO_UNDIRECTED_DESCRIPTION)
    public Stream<ToUndirectedMutateResult> mutate(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return facade.algorithms().miscellaneous().toUndirectedMutateStub().execute(graphName, configuration);
    }

    @Internal
    @Deprecated(forRemoval = true)
    @Procedure(
        value = "gds.beta.graph.relationships.toUndirected.estimate", mode = READ,
        deprecatedBy = "gds.graph.relationships.toUndirected.estimate"
    )
    @Description(MEMORY_ESTIMATION_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimateDeprecated(
        @Name(value = "graphNameOrConfiguration") Object graphNameOrConfiguration,
        @Name(value = "algoConfiguration") Map<String, Object> algoConfiguration
    ) {
        facade.deprecatedProcedures().called("gds.beta.graph.relationships.toUndirected.estimate");
        facade
            .log()
            .warn("Procedure `gds.beta.graph.relationships.toUndirected.estimate` has been deprecated, please use `gds.graph.relationships.toUndirected.estimate`.");
        return estimate(graphNameOrConfiguration, algoConfiguration);
    }

    @Procedure(value = "gds.graph.relationships.toUndirected.estimate", mode = READ)
    @Description(MEMORY_ESTIMATION_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphNameOrConfiguration") Object graphNameOrConfiguration,
        @Name(value = "algoConfiguration") Map<String, Object> algoConfiguration
    ) {
        return facade.algorithms().miscellaneous().toUndirectedMutateStub().estimate(graphNameOrConfiguration, algoConfiguration);
    }
}
