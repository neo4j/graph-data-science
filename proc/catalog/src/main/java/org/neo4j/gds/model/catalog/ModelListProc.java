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
package org.neo4j.gds.model.catalog;

import org.neo4j.gds.procedures.GraphDataScienceProcedures;
import org.neo4j.gds.procedures.modelcatalog.ModelCatalogResult;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Internal;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.stream.Stream;

import static org.neo4j.gds.procedures.modelcatalog.ModelCatalogProcedureFacade.NO_VALUE;
import static org.neo4j.procedure.Mode.READ;

public class ModelListProc {
    private static final String DESCRIPTION = "Lists all models contained in the model catalog.";

    @Context
    public GraphDataScienceProcedures facade;

    @Procedure(name = "gds.model.list", mode = READ)
    @Description(DESCRIPTION)
    public Stream<ModelCatalogResult> list(@Name(value = "modelName", defaultValue = NO_VALUE) String modelName) {
        return facade.modelCatalog().list(modelName);
    }

    @Procedure(name = "gds.beta.model.list", mode = READ, deprecatedBy = "gds.model.list")
    @Description(DESCRIPTION)
    @Deprecated
    @Internal
    public Stream<BetaModelCatalogResult> betaList(@Name(value = "modelName", defaultValue = NO_VALUE) String modelName) {
        facade.deprecatedProcedures().called("gds.beta.model.list");
        facade.log().warn("Procedure `gds.beta.model.list` has been deprecated, please use `gds.model.list`.");

        return list(modelName).map(BetaModelCatalogResult::new);
    }
}
