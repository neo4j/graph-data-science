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
package org.neo4j.gds.catalog;

import org.neo4j.gds.procedures.GraphDataScienceProcedureFacade;
import org.neo4j.gds.procedures.catalog.GraphInfo;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.stream.Stream;

import static org.neo4j.gds.procedures.catalog.GraphCatalogProcedureConstants.DROP_DESCRIPTION;
import static org.neo4j.procedure.Mode.READ;

public class GraphDropProc {
    @Context
    public GraphDataScienceProcedureFacade facade;

    public GraphDropProc() {
    }

    GraphDropProc(GraphDataScienceProcedureFacade facade) {
        this.facade = facade;
    }

    @Procedure(name = "gds.graph.drop", mode = READ)
    @Description(DROP_DESCRIPTION)
    public Stream<GraphInfo> dropGraph(
        @Name(value = "graphName") Object graphNameOrListOfGraphNames,
        @Name(value = "failIfMissing", defaultValue = "true") boolean failIfMissing,
        @Name(value = "dbName", defaultValue = "") String dbName,
        @Name(value = "username", defaultValue = "") String username
    ) {
        return facade.catalog().dropGraph(graphNameOrListOfGraphNames, failIfMissing, dbName, username);
    }
}
