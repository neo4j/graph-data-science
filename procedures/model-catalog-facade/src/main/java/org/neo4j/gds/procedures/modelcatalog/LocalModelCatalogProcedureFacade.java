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
package org.neo4j.gds.procedures.modelcatalog;

import org.neo4j.gds.applications.ApplicationsFacade;

import java.util.stream.Stream;

public class LocalModelCatalogProcedureFacade implements ModelCatalogProcedureFacade {

    private final ModelNameValidationService modelNameValidationService = new ModelNameValidationService();

    private final ApplicationsFacade applicationsFacade;

    public LocalModelCatalogProcedureFacade(ApplicationsFacade applicationsFacade) {
        this.applicationsFacade = applicationsFacade;
    }

    @Override
    public Stream<ModelCatalogResult> drop(String modelNameAsString, boolean failIfMissing) {
        var modelName = modelNameValidationService.validate(modelNameAsString);

        var model = applicationsFacade.modelCatalog().drop(modelName, failIfMissing);

        return Stream.ofNullable(model).map(ModelTransformer::toModelCatalogResult);
    }

    @Override
    public Stream<ModelExistsResult> exists(String modelNameAsString) {
        var modelName = modelNameValidationService.validate(modelNameAsString);

        var result = applicationsFacade.modelCatalog()
            .exists(modelName)
            .map(model -> new ModelExistsResult(model.name(), model.algoType(), true))
            .orElseGet(() -> new ModelExistsResult(modelNameAsString, "n/a", false));

        return Stream.of(result);
    }

    @Override
    public Stream<ModelCatalogResult> list(String modelName) {
        if (modelName == null || modelName.equals(NO_VALUE)) return list();

        return lookup(modelName);
    }

    private Stream<ModelCatalogResult> list() {
        var models = applicationsFacade.modelCatalog().list();

        return models.stream().map(ModelTransformer::toModelCatalogResult);
    }

    private Stream<ModelCatalogResult> lookup(String modelNameAsString) {
        var modelName = modelNameValidationService.validate(modelNameAsString);

        var model = applicationsFacade.modelCatalog().lookup(modelName);

        if (model == null) return Stream.empty();

        var result = ModelTransformer.toModelCatalogResult(model);

        return Stream.of(result);
    }
}
