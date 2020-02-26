/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.catalog;

import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

public class GraphDropProc extends CatalogProc {

    private static final String DESCRIPTION = "Drops a named graph from the catalog and frees up the resources it occupies.";

    @Procedure(name = "gds.graph.drop", mode = Mode.WRITE)
    @Description(DESCRIPTION)
    public Stream<GraphInfo> drop(@Name(value = "graphName") String graphName) {
        validateGraphName(graphName);

        AtomicReference<GraphInfo> result = new AtomicReference<>();
        GraphStoreCatalog.remove(getUsername(), graphName, (removedGraph) -> {
            result.set(new GraphInfo(removedGraph.config(), removedGraph.getGraph(), computeHistogram()));
        });

        return Stream.of(result.get());
    }

}
