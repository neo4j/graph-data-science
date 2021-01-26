/*
 * Copyright (c) 2017-2021 "Neo4j,"
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

package org.neo4j.graphalgo.model.catalog;

import org.neo4j.configuration.Config;
import org.neo4j.gds.model.PersistedModel;
import org.neo4j.gds.model.storage.ModelToFileExporter;
import org.neo4j.graphalgo.BaseProc;
import org.neo4j.graphalgo.compat.GraphDatabaseApiProxy;
import org.neo4j.graphalgo.core.ModelPersistenceSettings;
import org.neo4j.graphalgo.core.model.ModelCatalog;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import java.util.stream.Stream;

import static org.neo4j.graphalgo.core.utils.export.GraphStoreExporter.DIRECTORY_IS_WRITABLE;
import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;
import static org.neo4j.procedure.Mode.READ;

public class ModelPersistProc extends BaseProc {

    private static final String DESCRIPTION = "Persist the selected model to disk";

    @Procedure(name = "gds.alpha.model.persist", mode = READ)
    @Description(DESCRIPTION)
    public Stream<ModelPersistResult> persist(@Name(value = "modelName") String modelName) throws IOException {
        var model = ModelCatalog.getUntyped(username(), modelName);

        if (model.persisted()) {
            return Stream.of(new ModelPersistResult(modelName, 0));
        }

        var timer = ProgressTimer.start();

        var modelDir = createModelDir();

        ModelToFileExporter.toFile(
            modelDir,
            model
        );

        var persistedModel = PersistedModel.withInitialData(modelDir, model.data());
        ModelCatalog.setUnsafe(persistedModel);

        timer.stop();

        return Stream.of(new ModelPersistResult(modelName, timer.getDuration()));
    }

    private Path createModelDir() {
        var neo4jConfig = GraphDatabaseApiProxy.resolveDependency(api, Config.class);
        var persistenceDir = neo4jConfig.get(ModelPersistenceSettings.model_persistence_location);

        if (persistenceDir == null) {
            throw new RuntimeException(formatWithLocale(
                "The configuration option '%s' must be set.",
                ModelPersistenceSettings.model_persistence_location
            ));
        }

        DIRECTORY_IS_WRITABLE.validate(persistenceDir);

        var modelDir = persistenceDir.resolve(UUID.randomUUID().toString());

        try {
            Files.createDirectory(modelDir);
        } catch (IOException e) {
            throw new RuntimeException("Could not create persistence directory", e);
        }

        return modelDir;
    }

    public static class ModelPersistResult {
        public final String modelName;
        public final long persistMillis;

        ModelPersistResult(String modelName, long persistMillis) {
            this.modelName = modelName;
            this.persistMillis = persistMillis;
        }
    }
}
