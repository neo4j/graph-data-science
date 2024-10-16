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
package org.neo4j.gds.procedures.pipelines;

import org.neo4j.gds.applications.modelcatalog.ModelRepository;
import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.logging.Log;

/**
 * A small service that might ought to go under {@link org.neo4j.gds.applications.modelcatalog.ModelRepository}.
 */
class ModelPersister {
    private final Log log;
    private final ModelCatalog modelCatalog;
    private final ModelRepository modelRepository;

    ModelPersister(Log log, ModelCatalog modelCatalog, ModelRepository modelRepository) {
        this.log = log;
        this.modelCatalog = modelCatalog;
        this.modelRepository = modelRepository;
    }

    void persistModel(Model<?, ?, ?> model, boolean persistToDisk) {
        modelCatalog.set(model);

        if (!persistToDisk) return; // all done

        try {
            modelRepository.store(model);
        } catch (Exception e) { // this is terrifying engineering, inherited from ...
            log.error("Failed to store model to disk after training.", e);
            throw e;
        }
    }
}
