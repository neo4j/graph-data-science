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

import org.neo4j.gds.applications.algorithms.machinery.MemoryEstimateResult;
import org.neo4j.gds.procedures.GraphDataScienceProcedures;
import org.neo4j.gds.procedures.algorithms.similarity.SimilarityStreamResult;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Internal;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.procedures.ProcedureConstants.MEMORY_ESTIMATION_DESCRIPTION;
import static org.neo4j.gds.similarity.filteredknn.Constants.FILTERED_KNN_DESCRIPTION;
import static org.neo4j.procedure.Mode.READ;

public class FilteredKnnStreamProc {
    @Context
    public GraphDataScienceProcedures facade;

    @Procedure(value = "gds.knn.filtered.stream", mode = READ)
    @Description(FILTERED_KNN_DESCRIPTION)
    public Stream<SimilarityStreamResult> stream(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return facade.algorithms().similarity().filteredKnnStream(graphName, configuration);
    }

    @Procedure(value = "gds.knn.filtered.stream.estimate", mode = READ)
    @Description(MEMORY_ESTIMATION_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphNameOrConfiguration") Object graphNameOrConfiguration,
        @Name(value = "algoConfiguration") Map<String, Object> algoConfiguration
    ) {
        return facade.algorithms().similarity().filteredKnnStreamEstimate(graphNameOrConfiguration, algoConfiguration);
    }

    @Procedure(value = "gds.alpha.knn.filtered.stream", mode = READ, deprecatedBy = "gds.knn.filtered.stream")
    @Description(FILTERED_KNN_DESCRIPTION)
    @Internal
    @Deprecated
    public Stream<SimilarityStreamResult> alphaStream(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        facade.deprecatedProcedures().called("gds.alpha.knn.filtered.stream");
        facade
            .log()
            .warn(
                "Procedure `gds.alpha.knn.filtered.stream` has been deprecated, please use `gds.knn.filtered.stream`.");
        return stream(graphName, configuration);
    }
}
