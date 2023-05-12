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

import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;

public class ModelListProc extends ModelCatalogProc {

    private static final String DESCRIPTION = "Lists all models contained in the model catalog.";

    @Procedure(name = "gds.beta.model.list", mode = READ)
    @Description(DESCRIPTION)
    public Stream<ModelCatalogResult> list(@Name(value = "modelName", defaultValue = NO_VALUE) String modelName) {
        ModelCatalog modelCatalog = executionContext().modelCatalog();
        if (modelName == null || modelName.equals(NO_VALUE)) {
            var models = modelCatalog.list(username());
            return models.stream().map(ModelCatalogResult::new);
        } else {
            validateModelName(modelName);
            var model = modelCatalog.getUntyped(username(), modelName);
            return model == null
                ? Stream.empty()
                : Stream.of(new ModelCatalogResult(model));
        }
    }
}
