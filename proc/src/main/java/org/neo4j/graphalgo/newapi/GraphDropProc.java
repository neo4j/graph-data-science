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

import org.neo4j.graphalgo.BaseProc;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.loading.GraphCatalog;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.neo4j.graphalgo.newapi.GraphCatalogProcs.HISTOGRAM_FIELD_NAME;

public class GraphDropProc extends BaseProc {

    @Procedure(name = "algo.beta.graph.drop", mode = Mode.READ)
    @Description("CALL gds.graph.drop(" +
                 "  graphName: STRING" +
                 ") YIELD" +
                 "  graphName: STRING," +
                 "  nodeProjection: STRING," +
                 "  relationshipProjection: STRING," +
                 "  nodes: INTEGER," +
                 "  relationships: INTEGER")
    public Stream<GraphInfo> exists(@Name(value = "graphName", defaultValue = "null") String graphName) {
        CypherMapWrapper.failOnBlank("graphName", graphName);

        boolean computeHistogram = callContext.outputFields().anyMatch(HISTOGRAM_FIELD_NAME::equals);

        AtomicReference<GraphInfo> result = new AtomicReference<>();
        GraphCatalog.remove(getUsername(), graphName, (removedGraph) -> {
            result.set(new GraphInfo(removedGraph.config(), removedGraph.getGraph(), computeHistogram));
        });

        return Stream.of(result.get());
    }

}
