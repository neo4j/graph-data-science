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
package org.neo4j.graphalgo.core;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Config;
import org.neo4j.gds.model.PersistedModel;
import org.neo4j.gds.model.storage.ImmutableModelExportConfig;
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
public final class PersistedModelsExtension extends ExtensionFactory<PersistedModelsExtension.Dependencies> {

    public PersistedModelsExtension() {
        super(ExtensionType.DATABASE, "gds.enterprise");
    }

    @Override
    public Lifecycle newInstance(ExtensionContext context, Dependencies dependencies) {
        return new LifecycleAdapter() {
            @Override
            public void init() {
                var userLog = dependencies
                    .logService()
                    .getUserLog(getClass());

                var persistencePath = dependencies.config().get(ModelPersistenceSettings.model_persistence_location);

                if (!validatePath(persistencePath, userLog)) {
                    return;
                }

                try {
                    Files
                        .list(persistencePath)
                        .forEach(persistedModelPath -> openPersistedModel(userLog, persistedModelPath));
                } catch (IOException e) {
                    userLog.error("Could not list persisted model files", e);
                }
            }

            @Override
            public void shutdown() {
            }
        };
    }

    static void openPersistedModel(Log log, Path persistedModelPath) {
        if (Files.isDirectory(persistedModelPath)) {
            var config = ImmutableModelExportConfig
                .builder()
                .fileName("model")
                .build();

            PersistedModel model;
            try {
                model = new PersistedModel(persistedModelPath);
                if (ModelCatalog.exists(model.username(), model.name())) {
                    log.error(
                        "Cannot open persisted model %s for user %s from %s. A model with the same name already exists for that user.",
                        model.name(),
                        model.username(),
                        persistedModelPath
                    );
                } else {
                    ModelCatalog.set(model);
                }
            } catch (IOException e) {
                log.error(
                    formatWithLocale(
                        "Could not load model stored at %s",
                        persistedModelPath
                    ), e
                );
            }
        }
    }

    static boolean validatePath(Path persistencePath, Log log) {
        if (persistencePath == null) {
            return false;
        }

        if (!Files.exists(persistencePath)) {
            log.error(
                "The configured model persistence path '%s' does not exist. Cannot load or persist models.",
                persistencePath
            );
            return false;
        }

        if (!Files.isDirectory(persistencePath)) {
            log.error(
                "The configured model persistence path '%s' is not a directory. Cannot load or persist models.",
                persistencePath
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
