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

import org.neo4j.configuration.Config;
import org.neo4j.gds.model.StoredModel;
import org.neo4j.gds.model.storage.ModelToFileExporter;
import org.neo4j.graphalgo.BaseProc;
import org.neo4j.graphalgo.compat.GraphDatabaseApiProxy;
import org.neo4j.graphalgo.core.GdsEdition;
import org.neo4j.graphalgo.core.ModelStoreSettings;
import org.neo4j.graphalgo.core.model.Model;
import org.neo4j.graphalgo.core.model.ModelCatalog;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphdb.GraphDatabaseService;
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

public class ModelStoreProc extends BaseProc {

    private static final String DESCRIPTION = "Store the selected model to disk.";

    @Procedure(name = "gds.alpha.model.store", mode = READ)
    @Description(DESCRIPTION)
    public Stream<ModelStoreResult> store(@Name(value = "modelName") String modelName) throws IOException {
        if (!GdsEdition.instance().isOnEnterpriseEdition()) {
            throw new RuntimeException("Storing a model is only available with the Graph Data Science library Enterprise Edition.");
        }

        var model = ModelCatalog.getUntyped(username(), modelName);

        if (model.stored()) {
            return Stream.of(new ModelStoreResult(modelName, 0));
        }

        var timer = ProgressTimer.start();

        storeModel(api, model);

        timer.stop();

        return Stream.of(new ModelStoreResult(modelName, timer.getDuration()));
    }

    static void storeModel(GraphDatabaseService db, Model<?, ?> model) throws IOException {
        var modelDir = createModelDir(db);

        ModelToFileExporter.toFile(
            modelDir,
            model
        );

        var storedModel = StoredModel.withInitialData(modelDir, model.data());
        ModelCatalog.setUnsafe(storedModel);
    }

    private static Path createModelDir(GraphDatabaseService db) {
        var neo4jConfig = GraphDatabaseApiProxy.resolveDependency(db, Config.class);
        var storeDir = neo4jConfig.get(ModelStoreSettings.model_store_location);

        if (storeDir == null) {
            throw new RuntimeException(formatWithLocale(
                "The configuration option '%s' must be set.",
                ModelStoreSettings.model_store_location
            ));
        }

        DIRECTORY_IS_WRITABLE.validate(storeDir);

        var modelDir = storeDir.resolve(UUID.randomUUID().toString());

        try {
            Files.createDirectory(modelDir);
        } catch (IOException e) {
            throw new RuntimeException("Could not create model store directory.", e);
        }

        return modelDir;
    }

    public static class ModelStoreResult {
        public final String modelName;
        public final long storeMillis;

        ModelStoreResult(String modelName, long storeMillis) {
            this.modelName = modelName;
            this.storeMillis = storeMillis;
        }
    }
}
