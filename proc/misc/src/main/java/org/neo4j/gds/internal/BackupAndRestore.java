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
package org.neo4j.gds.internal;

import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.model.StoredModel;
import org.neo4j.gds.model.storage.ModelToFileExporter;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.config.GraphCreateFromStoreConfig;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;
import org.neo4j.graphalgo.core.model.ModelCatalog;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.io.file.CsvGraphStoreImporter;
import org.neo4j.graphalgo.core.utils.io.file.GraphStoreExporterUtil;
import org.neo4j.graphalgo.core.utils.io.file.ImmutableCsvGraphStoreImporterConfig;
import org.neo4j.graphalgo.core.utils.io.file.ImmutableGraphStoreToFileExporterConfig;
import org.neo4j.graphalgo.core.utils.io.file.csv.AutoloadFlagVisitor;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.logging.Log;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.neo4j.graphalgo.core.utils.io.GraphStoreExporter.DIRECTORY_IS_WRITABLE;
import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

public final class BackupAndRestore {

    private static final String GRAPHS_DIR = "graphs";
    private static final String MODELS_DIR = "models";

    @ValueClass
    interface BackupResult {

        String type();

        boolean done();

        @Nullable String path();

        long exportMillis();

        static BackupResult successfulGraph(Path path, ProgressTimer timer) {
            return ImmutableBackupResult.of("graph", true, path.toString(), timer.getDuration());
        }

        static BackupResult failedGraph() {
            return ImmutableBackupResult.of("graph", false, null, 0);
        }

        static BackupResult successfulModel(Path path, ProgressTimer timer) {
            return ImmutableBackupResult.of("model", true, path.toString(), timer.getDuration());
        }

        static BackupResult failedModel() {
            return ImmutableBackupResult.of("model", false, null, 0);
        }
    }


    static List<BackupResult> backup(
        Path backupRoot,
        Log log,
        long timeoutInSeconds,
        AllocationTracker allocationTracker
    ) {
        log.info("Preparing for backup");

        var result = new ArrayList<BackupResult>();

        var timer = ProgressTimer.start();
        try (timer) {
            result.addAll(backupGraphStores(backupRoot.resolve(GRAPHS_DIR), log, allocationTracker));
            result.addAll(backupModels(backupRoot.resolve(MODELS_DIR), log));
        }

        var elapsedTimeInSeconds = TimeUnit.MILLISECONDS.toSeconds(timer.getDuration());
        if (elapsedTimeInSeconds > timeoutInSeconds) {
            log.warn(
                "Backup took too long, the actual time of %d seconds is greater than the provided timeout of %d seconds",
                elapsedTimeInSeconds,
                timeoutInSeconds
            );
        } else {
            log.info(
                "Backup finished within the given timeout, it took %d seconds and the provided timeout was %d seconds.",
                elapsedTimeInSeconds,
                timeoutInSeconds
            );
        }
        return result;
    }

    static void restore(Path storePath, Log log) {
        var graphsPath = storePath.resolve(GRAPHS_DIR);
        var modelsPath = storePath.resolve(MODELS_DIR);
        if (Files.exists(graphsPath) && Files.exists(modelsPath)) {
            restoreGraphs(graphsPath, log);
            restoreModels(modelsPath, log);
        } else {
            restoreGraphs(storePath, log);
        }
    }

    private static List<BackupResult> backupGraphStores(
        Path backupRoot,
        Log log,
        AllocationTracker allocationTracker
    ) {
        DIRECTORY_IS_WRITABLE.validate(backupRoot);
        return GraphStoreCatalog.getAllGraphStores()
            .flatMap(store -> {
                var createConfig = store.config();
                var graphStore = store.graphStore();

                try {
                    var config = ImmutableGraphStoreToFileExporterConfig
                        .builder()
                        .includeMetaData(true)
                        .autoload(true)
                        .exportName(createConfig.graphName())
                        .username(store.userName())
                        .build();

                    var backupPath = GraphStoreExporterUtil.getExportPath(backupRoot, config);

                    var timer = ProgressTimer.start();
                    GraphStoreExporterUtil.runGraphStoreExportToCsv(
                        graphStore,
                        backupPath,
                        config,
                        log,
                        allocationTracker
                    );
                    timer.stop();

                    return Stream.of(BackupResult.successfulGraph(backupPath, timer));
                } catch (Exception e) {
                    log.warn(
                        formatWithLocale(
                            "Graph backup failed for graph '%s' for user '%s'",
                            createConfig.graphName(),
                            store.userName()
                        ),
                        e
                    );
                    return Stream.of(BackupResult.failedGraph());
                }
            }).collect(Collectors.toList());
    }

    private static List<BackupResult> backupModels(Path backupRoot, Log log) {
        DIRECTORY_IS_WRITABLE.validate(backupRoot);
        return ModelCatalog.getAllModels().flatMap(model -> {
            try {
                var modelRoot = backupRoot.resolve(UUID.randomUUID().toString());
                Files.createDirectory(modelRoot);

                var timer = ProgressTimer.start();
                ModelToFileExporter.toFile(modelRoot, model);
                timer.stop();

                return Stream.of(BackupResult.successfulModel(modelRoot, timer));
            } catch (Exception e) {
                log.warn(
                    formatWithLocale(
                        "Model backup failed on model '%s' for user '%s'",
                        model.name(),
                        model.creator()
                    ),
                    e
                );
                return Stream.of(BackupResult.failedModel());
            }
        }).collect(Collectors.toList());
    }

    private static void restoreGraphs(Path storePath, Log log) {
        try {
            getImportPaths(storePath)
                .filter(graphDir -> Files.exists(graphDir.resolve(AutoloadFlagVisitor.AUTOLOAD_FILE_NAME)))
                .forEach(path -> restoreGraph(path, log));
        } catch (Exception e) {
            log.warn("Graph restore failed", e);
        }
    }

    private static void restoreGraph(Path path, Log log) {
        var config = ImmutableCsvGraphStoreImporterConfig.builder().build();
        var graphStoreImporter = CsvGraphStoreImporter.create(config, path, log);

        graphStoreImporter.run(AllocationTracker.empty());

        var graphStore = graphStoreImporter.userGraphStore();

        var graphName = path.getFileName().toString();
        var createConfig = GraphCreateFromStoreConfig.emptyWithName(
            graphStore.userName(),
            graphName
        );
        GraphStoreCatalog.set(createConfig, graphStore.graphStore());
    }

    private static void restoreModels(Path storePath, Log log) {
        try {
            getImportPaths(storePath).forEach(path -> restoreModel(path, log));
        } catch (Exception e) {
            log.warn("Model restore failed", e);
        }
    }

    private static void restoreModel(Path storedModelPath, Log log) {
        try {
            var model = new StoredModel(storedModelPath);
            if (ModelCatalog.exists(model.creator(), model.name())) {
                log.error(
                    "Cannot open stored model %s for user %s from %s. A model with the same name already exists for that user.",
                    model.name(),
                    model.creator(),
                    storedModelPath
                );
            } else {
                ModelCatalog.set(model);
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

    private static Stream<Path> getImportPaths(Path storePath) throws IOException {
        return Files.list(storePath).peek(CsvGraphStoreImporter.DIRECTORY_IS_READABLE::validate);
    }

    private BackupAndRestore() {}
}
