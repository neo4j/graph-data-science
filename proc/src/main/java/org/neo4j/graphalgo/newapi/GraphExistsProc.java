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
import org.neo4j.graphalgo.core.loading.GraphCatalog;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.UserFunction;

import java.util.stream.Stream;

public class GraphExistsProc extends BaseProc {

    @Procedure(name = "algo.beta.graph.exists", mode = Mode.READ)
    @Description("CALL graph.exists(" +
                 "  graphName: STRING" +
                 ") YIELD" +
                 "  graphName: STRING," +
                 "  exists: BOOLEAN")
    public Stream<GraphExistsResult> exists(@Name(value = "graphName", defaultValue = "null") String graphName) {
        return Stream.of(new GraphExistsResult(graphName, GraphCatalog.exists(getUsername(), graphName)));
    }

    @UserFunction(name = "algo.beta.graph.exists")
    @Description("RETURN graph.exists(graphName: STRING) :: BOOLEAN")
    public boolean existsFunction(@Name(value = "graphName") String graphName) {
        return GraphCatalog.exists(getUsername(), graphName);
    }

    public static class GraphExistsResult {
        public final String graphName;
        public final boolean exists;

        GraphExistsResult(String graphName, boolean exists) {
            this.graphName = graphName;
            this.exists = exists;
        }
    }
}
