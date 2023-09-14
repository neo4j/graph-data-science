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
package org.neo4j.gds.beta.generator;

import org.neo4j.gds.applications.graphstorecatalog.GraphGenerationStats;
import org.neo4j.gds.procedures.GraphDataScience;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Internal;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;

public final class GraphGenerateProc {
    private static final String DESCRIPTION = "Computes a random graph, which will be stored in the graph catalog.";

    @Context
    public GraphDataScience facade;

    @Internal
    @Deprecated(forRemoval = true)
    @Procedure(name = "gds.beta.graph.generate", mode = READ, deprecatedBy = "gds.graph.generate")
    @Description(value = DESCRIPTION)
    public Stream<GraphGenerationStats> generateDeprecated(
        @Name(value = "graphName") String graphName,
        @Name(value = "nodeCount") long nodeCount,
        @Name(value = "averageDegree") long averageDegree,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        facade
            .log()
            .warn("Procedure `gds.beta.graph.generate` has been deprecated, please use `gds.graph.generate`.");

        return generate(graphName, nodeCount, averageDegree, configuration);
    }

    @Procedure(name = "gds.graph.generate", mode = READ)
    @Description(value = "Computes a random graph, which will be stored in the graph catalog.")
    public Stream<GraphGenerationStats> generate(
        @Name(value = "graphName") String graphName,
        @Name(value = "nodeCount") long nodeCount,
        @Name(value = "averageDegree") long averageDegree,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return facade.catalog().generateGraph(graphName, nodeCount, averageDegree, configuration);
    }
}
