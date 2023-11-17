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
package org.neo4j.gds.kmeans;

import org.neo4j.gds.BaseProc;
import org.neo4j.gds.procedures.GraphDataScience;
import org.neo4j.gds.procedures.community.kmeans.KmeansStreamResult;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Internal;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;

public class KmeansStreamProc extends BaseProc {
    @Context
    public GraphDataScience facade;

    @Procedure(value = "gds.kmeans.stream", mode = READ)
    @Description(Kmeans.KMEANS_DESCRIPTION)
    public Stream<KmeansStreamResult> stream(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return facade.community().kmeansStream(graphName, configuration);
    }

    @Deprecated(forRemoval = true)
    @Internal
    @Procedure(value = "gds.beta.kmeans.stream", mode = READ, deprecatedBy = "gds.kmeans.stream")
    @Description(Kmeans.KMEANS_DESCRIPTION)
    public Stream<KmeansStreamResult> betaStream(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        facade.deprecatedProcedures().called("gds.beta.kmeans.stream");
        facade.log()
            .warn("Procedure `gds.beta.kmeans.stream` has been deprecated, please use `gds.kmeans.stream`.");
        return stream(graphName, configuration);
    }

    @Procedure(value = "gds.kmeans.stream.estimate", mode = READ)
    @Description(ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphNameOrConfiguration") Object graphName,
        @Name(value = "algoConfiguration") Map<String, Object> configuration
    ) {
        var spec = new KmeansStreamSpec();

        return facade.community().kmeansEstimateStream(graphName, configuration);
    }

    @Deprecated(forRemoval = true)
    @Internal
    @Procedure(value = "gds.beta.kmeans.stream.estimate", mode = READ, deprecatedBy = "gds.kmeans.stream.estimate")
    @Description(ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> betaEstimate(
        @Name(value = "graphNameOrConfiguration") Object graphName,
        @Name(value = "algoConfiguration") Map<String, Object> configuration
    ) {
        facade.deprecatedProcedures().called("gds.beta.kmeans.stream.estimate");
        facade.log()
            .warn("Procedure `gds.beta.kmeans.stream.estimate` has been deprecated, please use `gds.kmeans.stream.estimate`.");
        return estimate(graphName, configuration);
    }
}
