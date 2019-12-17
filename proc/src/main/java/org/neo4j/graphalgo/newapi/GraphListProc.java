/*
 * Copyright (c) 2017-2019 "Neo4j,"
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

package org.neo4j.graphalgo.newapi;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.loading.GraphCatalog;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

public class GraphListProc extends CatalogProc {

    @Procedure(name = "algo.beta.graph.list", mode = Mode.READ)
    public Stream<GraphInfo> list(@Name(value = "graphName", defaultValue = "null") Object graphName) {
        if (Objects.nonNull(graphName) && !(graphName instanceof String)) {
            throw new IllegalArgumentException("`graphName` parameter must be a STRING");
        }
        String graphNameS = (String) graphName;

        Stream<Map.Entry<GraphCreateConfig, Graph>> graphEntries = GraphCatalog
            .getLoadedGraphs(getUsername())
            .entrySet()
            .stream();

        if (graphNameS != null) {
            validateGraphName(graphNameS);

            // we should only list the provided graph
            graphEntries = graphEntries.filter(e -> e.getKey().graphName().equals(graphNameS));
        }

        return graphEntries.map(e -> new GraphInfo(e.getKey(), e.getValue(), computeHistogram()));
    }

}
