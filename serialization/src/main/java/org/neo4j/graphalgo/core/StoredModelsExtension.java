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
package org.neo4j.graphalgo.core;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Config;
import org.neo4j.gds.model.StoredModel;
import org.neo4j.graphalgo.core.model.ModelCatalog;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.ExtensionType;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.internal.LogService;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

@ServiceProvider
public final class StoredModelsExtension extends ExtensionFactory<StoredModelsExtension.Dependencies> {

    public StoredModelsExtension() {
        super(ExtensionType.DATABASE, "gds.model.store");
    }

    @Override
    public Lifecycle newInstance(ExtensionContext context, Dependencies dependencies) {
        return new LifecycleAdapter() {
            @Override
            public void init() {
                var userLog = dependencies
                    .logService()
                    .getUserLog(getClass());

                var storePath = dependencies.config().get(ModelStoreSettings.model_store_location);

                if (!validatePath(storePath, userLog)) {
                    return;
                }

                try {
                    Files
                        .list(storePath)
                        .forEach(persistedModelPath -> openStoredModel(userLog, persistedModelPath));
                } catch (IOException e) {
                    userLog.error("Could not list persisted model files", e);
                }
            }

            @Override
            public void shutdown() {
            }
        };
    }

    static void openStoredModel(Log log, Path storedModelPath) {
        if (Files.isDirectory(storedModelPath)) {
            StoredModel model;
            try {
                model = new StoredModel(storedModelPath);
                try {
                    ModelCatalog.set(model);
                } catch (IllegalArgumentException e) {
                    log.error(
                        "Cannot open stored model %s for user %s from %s. A model with the same name already exists for that user.",
                        model.name(),
                        model.creator(),
                        storedModelPath
                    );
                }
            } catch (IOException e) {
                log.error(
                    formatWithLocale(
                        "Could not load model stored at %s",
                        storedModelPath
                    ), e
                );
            }
        }
    }

    static boolean validatePath(Path storePath, Log log) {
        if (storePath == null) {
            return false;
        }

        if (!Files.exists(storePath)) {
            log.error(
                "The configured model store path '%s' does not exist. Cannot load or store models.",
                storePath
            );
            return false;
        }

        if (!Files.isDirectory(storePath)) {
            log.error(
                "The configured model store path '%s' is not a directory. Cannot load or store models.",
                storePath
            );
            return false;
        }

        return true;
    }

    interface Dependencies {
        Config config();

        LogService logService();
    }
}
