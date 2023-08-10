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

import org.neo4j.gds.procedures.catalog.GraphStoreCatalogProcedureFacade;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.stream.Stream;

import static org.neo4j.gds.procedures.catalog.GraphCatalogProcedureConstants.LIST_DESCRIPTION;
import static org.neo4j.gds.procedures.catalog.GraphCatalogProcedureConstants.NO_VALUE_PLACEHOLDER;
import static org.neo4j.procedure.Mode.READ;

public class GraphListProc {
    @Context
    public GraphStoreCatalogProcedureFacade facade;

    public GraphListProc() {

    }

    GraphListProc(GraphStoreCatalogProcedureFacade facade) {
        this.facade = facade;
    }

    @Procedure(name = "gds.graph.list", mode = READ)
    @Description(LIST_DESCRIPTION)
    public Stream<GraphInfoWithHistogram> listGraphs(
        @Name(value = "graphName", defaultValue = NO_VALUE_PLACEHOLDER) String graphName
    ) {
        return facade.listGraphs(graphName);
    }
}
