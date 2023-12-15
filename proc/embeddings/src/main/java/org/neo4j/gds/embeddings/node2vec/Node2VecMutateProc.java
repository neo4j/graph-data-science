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
package org.neo4j.gds.embeddings.node2vec;

import org.neo4j.gds.BaseProc;
import org.neo4j.gds.procedures.GraphDataScience;
import org.neo4j.gds.procedures.embeddings.node2vec.Node2VecMutateResult;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Internal;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;

public class Node2VecMutateProc {

    @Context
    public GraphDataScience facade;

    @Procedure(value = "gds.node2vec.mutate", mode = READ)
    @Description(Node2VecCompanion.DESCRIPTION)
    public Stream<Node2VecMutateResult> mutate(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return facade.nodeEmbeddings().node2VecMutate(graphName, configuration);
    }

    @Procedure(value = "gds.node2vec.mutate.estimate", mode = READ)
    @Description(BaseProc.ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphNameOrConfiguration") Object graphNameOrConfiguration,
        @Name(value = "algoConfiguration") Map<String, Object> algoConfiguration
    ) {
        return facade.nodeEmbeddings().node2VecMutateEstimate(graphNameOrConfiguration, algoConfiguration);
    }

    @Procedure(value = "gds.beta.node2vec.mutate", mode = READ, deprecatedBy = "gds.node2vec.mutate")
    @Description(Node2VecCompanion.DESCRIPTION)
    @Internal
    @Deprecated
    public Stream<Node2VecMutateResult> betaMutate(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        facade
            .deprecatedProcedures().called("gds.beta.node2vec.mutate");

        facade
            .log()
            .warn(
                "Procedure `gds.beta.node2vec.mutate` has been deprecated, please use `gds.node2vec.mutate`.");
        return mutate(graphName, configuration);
    }

    @Procedure(
        value = "gds.beta.node2vec.mutate.estimate", mode = READ, deprecatedBy = "gds.node2vec.mutate.estimate"
    )
    @Description(BaseProc.ESTIMATE_DESCRIPTION)
    @Internal
    @Deprecated
    public Stream<MemoryEstimateResult> betaEstimate(
        @Name(value = "graphNameOrConfiguration") Object graphNameOrConfiguration,
        @Name(value = "algoConfiguration") Map<String, Object> algoConfiguration
    ) {
        facade
            .deprecatedProcedures().called("gds.beta.node2vec.mutate.estimate");

        facade
            .log()
            .warn(
                "Procedure `gds.beta.node2vec.mutate.estimate` has been deprecated, please use `gds.node2vec.mutate.estimate`.");
        return estimate(graphNameOrConfiguration, algoConfiguration);
    }
}
