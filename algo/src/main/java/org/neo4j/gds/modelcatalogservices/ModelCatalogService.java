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
package org.neo4j.gds.modelcatalogservices;

import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.logging.Log;
import org.neo4j.graphdb.GraphDatabaseService;

public class ModelCatalogService {

    private final ModelCatalog modelCatalog;
    private final GraphDatabaseService graphDatabaseService;
    private final Log log;

    public ModelCatalogService(ModelCatalog modelCatalog, GraphDatabaseService graphDatabaseService, Log log) {
        this.graphDatabaseService = graphDatabaseService;
        this.modelCatalog = modelCatalog;
        this.log = log;
    }

    public void set(Model model) {
        modelCatalog.set(model);

    }

    public ModelCatalog get() {
        return modelCatalog;
    }

    public void storeModelToDisk(Model model) {
        try {
            // FIXME: This works but is not what we want to do!

            modelCatalog.checkLicenseBeforeStoreModel(graphDatabaseService, "Store a model");
            var modelDir = modelCatalog.getModelDirectory(graphDatabaseService);
            modelCatalog.store(model.creator(), model.name(), modelDir);
        } catch (Exception e) {
            log.info("Failed to store model to disk after training.", e.getMessage());
            throw e;
        }
    }

}
