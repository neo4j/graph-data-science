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
package org.neo4j.gds.tlp;

import org.neo4j.gds.procedures.GraphDataScienceProcedures;
import org.neo4j.graphdb.Node;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Internal;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

import java.util.Map;

public class TopologicalLinkPredictionFunctions {
    @Context
    public GraphDataScienceProcedures facade;

    /**
     * <a href="https://en.wikipedia.org/wiki/Adamic/Adar_index">Adar index</a>
     */
    @UserFunction("gds.linkprediction.adamicAdar")
    @Description("Given two nodes, calculate Adamic Adar similarity")
    public double adamicAdarSimilarity(
        @Name("node1") Node node1,
        @Name("node2") Node node2,
        @Name(value = "config", defaultValue = "{}") Map<String, Object> config
    ) {
        return facade.functions().adamicAdarIndex(node1, node2, config);
    }

    @UserFunction(value = "gds.alpha.linkprediction.adamicAdar", deprecatedBy = "gds.linkprediction.adamicAdar")
    @Description("Given two nodes, calculate Adamic Adar similarity")
    @Internal
    @Deprecated
    public double alphaAdamicAdarSimilarity(
        @Name("node1") Node node1, @Name("node2") Node node2,
        @Name(value = "config", defaultValue = "{}") Map<String, Object> config
    ) {
        facade.deprecatedProcedures().called("gds.alpha.linkprediction.adamicAdar");
        facade.log().warn(
            "Function `gds.alpha.linkprediction.adamicAdar` has been deprecated, please use `gds.linkprediction.adamicAdar`.");

        return adamicAdarSimilarity(node1, node2, config);
    }
}
