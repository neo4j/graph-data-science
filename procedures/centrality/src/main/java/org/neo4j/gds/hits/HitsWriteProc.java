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
package org.neo4j.gds.hits;

import org.neo4j.gds.applications.algorithms.machinery.MemoryEstimateResult;
import org.neo4j.gds.procedures.GraphDataScienceProcedures;
import org.neo4j.gds.procedures.algorithms.centrality.HitsWriteResult;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Internal;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.hits.Constants.HITS_DESCRIPTION;
import static org.neo4j.gds.procedures.ProcedureConstants.MEMORY_ESTIMATION_DESCRIPTION;
import static org.neo4j.procedure.Mode.READ;
import static org.neo4j.procedure.Mode.WRITE;

public class HitsWriteProc {
    @Context
    public GraphDataScienceProcedures facade;

    @Procedure(name = "gds.hits.write", mode = WRITE)
    @Description(HITS_DESCRIPTION)
    public Stream<HitsWriteResult> write(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return facade.algorithms().centrality().hitsWrite(graphName, configuration);
    }

    @Procedure(name = "gds.hits.write.estimate", mode = READ)
    @Description(MEMORY_ESTIMATION_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphName") Object graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return facade.algorithms().centrality().hitsWriteEstimate(graphName, configuration);
    }

    @Procedure(
        name = "gds.alpha.hits.write",
        mode = WRITE,
        deprecatedBy = "gds.hits.write"
    )
    @Internal
    @Description(HITS_DESCRIPTION)
    @Deprecated
    public Stream<HitsWriteResult> alphaStream(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        facade.deprecatedProcedures().called("gds.alpha.hits.write");
        facade
            .log()
            .warn(
                "Procedure `gds.alpha.hits.write has been deprecated, please use `gds.hits.write`.");

        return write(graphName, configuration);
    }

    @Procedure(
        name = "gds.alpha.hits.write.estimate",
        mode = READ,
        deprecatedBy = "gds.hits.write.estimate"
    )
    @Internal
    @Description(MEMORY_ESTIMATION_DESCRIPTION)
    @Deprecated
    public Stream<MemoryEstimateResult> alphaEstimate(
        @Name(value = "graphName") Object graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        facade.deprecatedProcedures().called("gds.alpha.hits.write.estimate");
        facade.log()
            .warn(
                "Procedure `gds.alpha.hits.write.estimate has been deprecated, please use `gds.hits.write.estimate`.");

        return estimate(graphName, configuration);
    }
}
