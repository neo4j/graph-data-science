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

import static org.neo4j.gds.tlp.Constants.ADAMIC_ADAR_INDEX_DESCRIPTION;
import static org.neo4j.gds.tlp.Constants.COMMON_NEIGHBOURS_DESCRIPTION;
import static org.neo4j.gds.tlp.Constants.RESOURCE_ALLOCATION_SIMILARITY_DESCRIPTION;

public class TopologicalLinkPredictionFunctions {
    @Context
    public GraphDataScienceProcedures facade;

    @UserFunction("gds.linkprediction.adamicAdar")
    @Description(ADAMIC_ADAR_INDEX_DESCRIPTION)
    public double adamicAdarSimilarity(
        @Name("node1") Node node1,
        @Name("node2") Node node2,
        @Name(value = "config", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return facade.functions().adamicAdarIndex(node1, node2, configuration);
    }

    @UserFunction(value = "gds.alpha.linkprediction.adamicAdar", deprecatedBy = "gds.linkprediction.adamicAdar")
    @Description(ADAMIC_ADAR_INDEX_DESCRIPTION)
    @Internal
    @Deprecated
    public double alphaAdamicAdarSimilarity(
        @Name("node1") Node node1,
        @Name("node2") Node node2,
        @Name(value = "config", defaultValue = "{}") Map<String, Object> configuration
    ) {
        facade.deprecatedProcedures().called("gds.alpha.linkprediction.adamicAdar");
        facade.log().warn(
            "Function `gds.alpha.linkprediction.adamicAdar` has been deprecated, please use `gds.linkprediction.adamicAdar`.");

        return adamicAdarSimilarity(node1, node2, configuration);
    }

    @UserFunction("gds.linkprediction.commonNeighbors")
    @Description(COMMON_NEIGHBOURS_DESCRIPTION)
    public double commonNeighbors(
        @Name("node1") Node node1,
        @Name("node2") Node node2,
        @Name(value = "config", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return facade.functions().commonNeighbours(node1, node2, configuration);
    }

    @UserFunction(value = "gds.alpha.linkprediction.commonNeighbors", deprecatedBy = "gds.linkprediction.commonNeighbors")
    @Description(COMMON_NEIGHBOURS_DESCRIPTION)
    @Internal
    @Deprecated
    public double alphaCommonNeighbors(
        @Name("node1") Node node1,
        @Name("node2") Node node2,
        @Name(value = "config", defaultValue = "{}") Map<String, Object> configuration
    ) {
        facade.deprecatedProcedures().called("gds.alpha.linkprediction.commonNeighbors");
        facade.log().warn(
            "Function `gds.alpha.linkprediction.commonNeighbors` has been deprecated, please use `gds.linkprediction.commonNeighbors`.");

        return commonNeighbors(node1, node2, configuration);
    }

    @UserFunction("gds.linkprediction.resourceAllocation")
    @Description(RESOURCE_ALLOCATION_SIMILARITY_DESCRIPTION)
    public double resourceAllocationSimilarity(
        @Name("node1") Node node1,
        @Name("node2") Node node2,
        @Name(value = "config", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return facade.functions().resourceAllocationSimilarity(node1, node2, configuration);
    }

    @UserFunction(value = "gds.alpha.linkprediction.resourceAllocation", deprecatedBy = "gds.linkprediction.resourceAllocation")
    @Description(RESOURCE_ALLOCATION_SIMILARITY_DESCRIPTION)
    @Internal
    @Deprecated
    public double alphaResourceAllocationSimilarity(
        @Name("node1") Node node1,
        @Name("node2") Node node2,
        @Name(value = "config", defaultValue = "{}") Map<String, Object> configuration
    ) {
        facade.deprecatedProcedures().called("gds.alpha.linkprediction.resourceAllocation");
        facade.log().warn(
            "Function `gds.alpha.linkprediction.resourceAllocation` has been deprecated, please use `gds.linkprediction.resourceAllocation`.");

        return resourceAllocationSimilarity(node1, node2, configuration);
    }
}
