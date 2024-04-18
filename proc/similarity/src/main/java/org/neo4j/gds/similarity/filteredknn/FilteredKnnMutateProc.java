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
package org.neo4j.gds.similarity.filteredknn;

import org.neo4j.gds.BaseProc;
import org.neo4j.gds.procedures.GraphDataScienceProcedures;
import org.neo4j.gds.procedures.algorithms.similarity.KnnMutateResult;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Internal;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.procedures.ProcedureConstants.MEMORY_ESTIMATION_DESCRIPTION;
import static org.neo4j.procedure.Mode.READ;

public class FilteredKnnMutateProc extends BaseProc {

    @Context
    public GraphDataScienceProcedures facade;
    @Procedure(name = "gds.knn.filtered.mutate", mode = READ)
    @Description(FilteredKnnConstants.PROCEDURE_DESCRIPTION)
    public Stream<KnnMutateResult> mutate(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return facade.similarity().filteredKnnMutate(graphName, configuration);
    }

    @Procedure(value = "gds.knn.filtered.mutate.estimate", mode = READ)
    @Description(MEMORY_ESTIMATION_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphNameOrConfiguration") Object graphNameOrConfiguration,
        @Name(value = "algoConfiguration") Map<String, Object> algoConfiguration
    ) {
        return facade.similarity().filteredKnnMutateEstimate(graphNameOrConfiguration, algoConfiguration);
    }

    @Procedure(name = "gds.alpha.knn.filtered.mutate", mode = READ, deprecatedBy = "gds.knn.filtered.mutate")
    @Description(FilteredKnnConstants.PROCEDURE_DESCRIPTION)
    @Internal
    @Deprecated
    public Stream<KnnMutateResult> alphaMutate(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        executionContext()
            .metricsFacade()
            .deprecatedProcedures().called("gds.alpha.knn.filtered.mutate");

        executionContext()
            .log()
            .warn(
                "Procedure `gds.alpha.knn.filtered.mutate` has been deprecated, please use `gds.knn.filtered.mutate`.");
        return mutate(graphName, configuration);
    }
}
