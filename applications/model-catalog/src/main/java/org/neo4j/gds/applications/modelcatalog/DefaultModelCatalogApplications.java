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
package org.neo4j.gds.applications.modelcatalog;

import org.neo4j.gds.api.User;
import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.core.model.ModelCatalog;

import java.util.Optional;

public final class DefaultModelCatalogApplications implements ModelCatalogApplications {
    private final ModelCatalog modelCatalog;
    private final User user;

    private DefaultModelCatalogApplications(ModelCatalog modelCatalog, User user) {
        this.modelCatalog = modelCatalog;
        this.user = user;
    }

    public static DefaultModelCatalogApplications create(ModelCatalog modelCatalog, User user) {
        return new DefaultModelCatalogApplications(modelCatalog, user);
    }

    @Override
    public Model<?, ?, ?> drop(ModelName modelName, boolean failIfMissing) {
        if (failIfMissing) return modelCatalog.dropOrThrow(user.getUsername(), modelName.getValue());

        return modelCatalog.drop(user.getUsername(), modelName.getValue());
    }

    @Override
    public ModelExistsResult exists(ModelName modelName) {
        var untypedModel = modelCatalog.getUntyped(user.getUsername(), modelName.getValue());

        var modelType = Optional.ofNullable(untypedModel).map(Model::algoType).orElse("n/a");

        var exists = modelCatalog.exists(user.getUsername(), modelName.getValue());

        return new ModelExistsResult(modelName.getValue(), modelType, exists);
    }
}
