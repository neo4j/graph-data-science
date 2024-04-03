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
package org.neo4j.gds.similarity.filterednodesim;

import org.neo4j.gds.BaseProc;
import org.neo4j.gds.procedures.GraphDataScienceProcedures;
import org.neo4j.gds.procedures.similarity.SimilarityWriteResult;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Internal;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.procedures.ProcedureConstants.MEMORY_ESTIMATION_DESCRIPTION;
import static org.neo4j.gds.similarity.filterednodesim.FilteredNodeSimilarityStreamProc.DESCRIPTION;
import static org.neo4j.procedure.Mode.READ;
import static org.neo4j.procedure.Mode.WRITE;

public class FilteredNodeSimilarityWriteProc extends BaseProc {

    @Context
    public GraphDataScienceProcedures facade;

    @Procedure(value = "gds.nodeSimilarity.filtered.write", mode = WRITE)
    @Description(DESCRIPTION)
    public Stream<SimilarityWriteResult> write(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ){
        return facade.similarity().filteredNodeSimilarityWrite(graphName, configuration);
    }

    @Procedure(value = "gds.nodeSimilarity.filtered.write.estimate", mode = READ)
    @Description(MEMORY_ESTIMATION_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphNameOrConfiguration") Object graphNameOrConfiguration,
        @Name(value = "algoConfiguration") Map<String, Object> algoConfiguration
    ) {
        return facade.similarity().filteredNodeSimilarityEstimateWrite(graphNameOrConfiguration, algoConfiguration);
    }

    @Deprecated(forRemoval = true)
    @Internal
    @Procedure(value = "gds.alpha.nodeSimilarity.filtered.write", mode = WRITE, deprecatedBy = "gds.nodeSimilarity.filtered.write")
    @Description(DESCRIPTION)
    public Stream<SimilarityWriteResult> writeAlpha(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ){
        executionContext()
            .metricsFacade()
            .deprecatedProcedures().called("gds.alpha.nodeSimilarity.filtered.write");
        executionContext()
            .log()
            .warn("Procedure `gds.alpha.nodeSimilarity.filtered.write` has been deprecated, please use `gds.nodeSimilarity.filtered.write`.");

        return write(graphName, configuration);
    }

    @Deprecated(forRemoval = true)
    @Internal
    @Procedure(value = "gds.alpha.nodeSimilarity.filtered.write.estimate", mode = READ, deprecatedBy = "gds.nodeSimilarity.filtered.write.estimate")
    @Description(ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimateAlpha(
        @Name(value = "graphNameOrConfiguration") Object graphNameOrConfiguration,
        @Name(value = "algoConfiguration") Map<String, Object> algoConfiguration
    ) {
        executionContext()
            .metricsFacade()
            .deprecatedProcedures().called("gds.alpha.nodeSimilarity.filtered.write.estimate");
        executionContext()
            .log()
            .warn("Procedure `gds.alpha.nodeSimilarity.filtered.write.estimate` has been deprecated, please use `gds.nodeSimilarity.filtered.write.estimate`.");

        return estimate(graphNameOrConfiguration, algoConfiguration);
    }
    
}
