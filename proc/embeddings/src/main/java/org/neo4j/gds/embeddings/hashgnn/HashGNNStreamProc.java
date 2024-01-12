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
package org.neo4j.gds.embeddings.hashgnn;

import org.neo4j.gds.procedures.GraphDataScience;
import org.neo4j.gds.procedures.embeddings.hashgnn.HashGNNStreamResult;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Internal;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.embeddings.hashgnn.HashGNNProcCompanion.DESCRIPTION;
import static org.neo4j.procedure.Mode.READ;

public class HashGNNStreamProc {

    @Context
    public GraphDataScience facade;

    @Procedure(value = "gds.hashgnn.stream", mode = READ)
    @Description(DESCRIPTION)
    public Stream<HashGNNStreamResult> stream(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return facade.nodeEmbeddings().HashGNNStream(graphName, configuration);
    }

    @Internal
    @Deprecated(forRemoval = true)
    @Procedure(value = "gds.beta.hashgnn.stream", deprecatedBy = "gds.hashgnn.stream", mode = READ)
    @Description(DESCRIPTION)
    public Stream<HashGNNStreamResult> betaStream(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        facade
            .deprecatedProcedures().called("gds.beta.hashgnn.stream");

        facade.log()
            .warn("Procedure `gds.beta.hashgnn.stream` has been deprecated, please use `gds.hashgnn.stream`.");

        return stream(graphName, configuration);
    }

    @Procedure(value = "gds.hashgnn.stream.estimate", mode = READ)
    @Description(DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphNameOrConfiguration") Object graphNameOrConfiguration,
        @Name(value = "algoConfiguration") Map<String, Object> algoConfiguration
    ) {
        return facade.nodeEmbeddings().HashGNNStreamEstimate(graphNameOrConfiguration, algoConfiguration);
    }

    @Internal
    @Deprecated(forRemoval = true)
    @Procedure(value = "gds.beta.hashgnn.stream.estimate", deprecatedBy = "gds.hashgnn.stream.estimate", mode = READ)
    @Description(DESCRIPTION)
    public Stream<MemoryEstimateResult> betaEstimate(
        @Name(value = "graphNameOrConfiguration") Object graphNameOrConfiguration,
        @Name(value = "algoConfiguration") Map<String, Object> algoConfiguration
    ) {
        facade
            .deprecatedProcedures().called("gds.beta.hashgnn.stream.estimate");

        facade.log()
            .warn("Procedure `gds.beta.hashgnn.stream.estimate` has been deprecated, please use `gds.hashgnn.stream.estimate`.");

        return estimate(graphNameOrConfiguration, algoConfiguration);
    }
}
