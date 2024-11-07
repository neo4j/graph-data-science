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
import org.neo4j.gds.procedures.algorithms.centrality.HitsStreamResult;
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

public class HitsStreamProc {
    @Context
    public GraphDataScienceProcedures facade;

    @Procedure(name = "gds.hits.stream", mode = READ)
    @Description(HITS_DESCRIPTION)
    public Stream<HitsStreamResult> stream(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return facade.algorithms().centrality().hitsStream(graphName, configuration);
    }

    @Procedure(name = "gds.hits.stream.estimate", mode = READ)
    @Description(MEMORY_ESTIMATION_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphName") Object graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return facade.algorithms().centrality().hitsStreamEstimate(graphName, configuration);
    }

    @Procedure(
        name = "gds.alpha.hits.stream",
        mode = READ,
        deprecatedBy = "gds.hits.stream"
    )
    @Internal
    @Description(HITS_DESCRIPTION)
    @Deprecated
    public Stream<HitsStreamResult> alphaStream(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        facade.deprecatedProcedures().called("gds.alpha.hits.stream");
        facade
            .log()
            .warn(
                "Procedure `gds.alpha.hits.stream has been deprecated, please use `gds.hits.stream`.");

        return stream(graphName, configuration);
    }

    @Procedure(
        name = "gds.alpha.hits.stream.estimate",
        mode = READ,
        deprecatedBy = "gds.hits.stream.estimate"
    )
    @Internal
    @Description(MEMORY_ESTIMATION_DESCRIPTION)
    @Deprecated
    public Stream<MemoryEstimateResult> alphaEstimate(
        @Name(value = "graphName") Object graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        facade.deprecatedProcedures().called("gds.alpha.hits.stream.estimate");
        facade.log()
            .warn(
                "Procedure `gds.alpha.hits.stream.estimate has been deprecated, please use `gds.hits.stream.estimate`.");

        return estimate(graphName, configuration);
    }
}
