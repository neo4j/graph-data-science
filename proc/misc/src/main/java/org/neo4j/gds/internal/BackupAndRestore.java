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
import org.neo4j.graphalgo.core.model.Model;
import org.neo4j.graphalgo.core.model.ModelCatalog;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.io.file.CsvGraphStoreImporter;
import org.neo4j.graphalgo.core.utils.io.file.GraphStoreExporterUtil;
import org.neo4j.graphalgo.core.utils.io.file.ImmutableCsvGraphStoreImporterConfig;
import org.neo4j.graphalgo.core.utils.io.file.ImmutableGraphStoreToFileExporterConfig;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.utils.ExceptionUtil;
import org.neo4j.graphalgo.utils.StringFormatting;
import org.neo4j.logging.Log;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.function.Predicate.not;
import static org.neo4j.graphalgo.core.utils.io.GraphStoreExporter.DIRECTORY_IS_WRITABLE;
import static org.neo4j.graphalgo.core.utils.io.file.CsvGraphStoreImporter.DIRECTORY_IS_READABLE;
import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

public final class BackupAndRestore {

    private static final String GRAPHS_DIR = "graphs";
    private static final String MODELS_DIR = "models";

    @ValueClass
    interface BackupResult {

        String username();

        String type();

        boolean done();

        @Nullable String path();

        long exportMillis();

        static BackupResult successfulGraph(String username, Path path, ProgressTimer timer) {
            return ImmutableBackupResult.of(username, "graph", true, path.toString(), timer.getDuration());
        }

        static BackupResult failedGraph(String username) {
            return ImmutableBackupResult.of(username, "graph", false, null, 0);
        }

        static BackupResult successfulModel(String username, Path path, ProgressTimer timer) {
            return ImmutableBackupResult.of(username, "model", true, path.toString(), timer.getDuration());
        }

        static BackupResult failedModel(String username) {
            return ImmutableBackupResult.of(username, "model", false, null, 0);
        }
    }


    static List<BackupResult> backup(
        Path backupRoot,
        Log log,
        long timeoutInSeconds,
        AllocationTracker allocationTracker,
        @SuppressWarnings("SameParameterValue") String taskName
    ) {
        return backup(
            backupRoot,
            log,
            timeoutInSeconds,
            ignore -> {},
            ignore -> {},
            allocationTracker,
            taskName
        );
    }

    static List<BackupResult> backup(
        Path backupRoot,
        Log log,
        long timeoutInSeconds,
        Consumer<GraphStoreCatalog.GraphStoreWithUserNameAndConfig> graphOnSuccess,
        Consumer<Model<?, ?>> modelOnSuccess,
        AllocationTracker allocationTracker,
        String taskName
    ) {
        log.info("Preparing for %s", StringFormatting.toLowerCaseWithLocale(taskName));

        DIRECTORY_IS_WRITABLE.validate(backupRoot);

        var result = new ArrayList<BackupResult>();

        var timer = ProgressTimer.start();
        try (timer) {
            result.addAll(backupGraphStores(
                backupRoot,
                graphOnSuccess,
                log,
                allocationTracker
            ));
            result.addAll(backupModels(
                backupRoot,
                modelOnSuccess,
                log
            ));

            // We have the contract that both graphs and models folder exist, even if they do not contain any data
            result.stream().flatMap(backupResult -> {
                var userPath = backupRoot.resolve(backupResult.username());
                return Stream.of(userPath.resolve(GRAPHS_DIR), userPath.resolve(MODELS_DIR));
            }).distinct().forEach(pathToCreate -> {
                try {
                    Files.createDirectories(pathToCreate);
                } catch (IOException e) {
                    log.error(
                        formatWithLocale(
                            "Failed to create %s directory '%s'",
                            StringFormatting.toLowerCaseWithLocale(taskName),
                            pathToCreate
                        ),
                        e
                    );
                }
            });
        }

        var elapsedTimeInSeconds = TimeUnit.MILLISECONDS.toSeconds(timer.getDuration());
        if (elapsedTimeInSeconds > timeoutInSeconds) {
            log.warn(
                "%s took too long, the actual time of %d seconds is greater than the provided timeout of %d seconds",
                taskName,
                elapsedTimeInSeconds,
                timeoutInSeconds
            );
        } else {
            log.info(
                "%s finished within the given timeout, it took %d seconds and the provided timeout was %d seconds.",
                taskName,
                elapsedTimeInSeconds,
                timeoutInSeconds
            );
        }
        return result;
    }

    static void restore(Path restorePath, Path backupPath, Log log)  {
        var successfulRestorePaths = subdirectories(restorePath, log)
            .flatMap(userRestorePath -> {
                try {
                    restoreForUser(userRestorePath, log);
                    return Stream.of(userRestorePath);
                } catch (Exception e) {
                    log.error(formatWithLocale("Restore failed for user path '%s'", userRestorePath), e);
                    return Stream.empty();
                }
            })
            .collect(Collectors.toList());

        if (!successfulRestorePaths.isEmpty()) {
            try {
                createNewBackupFromRestore(backupPath, successfulRestorePaths);
            } catch (IOException e) {
                log.error(
                    formatWithLocale(
                        "Could not move the restored files to the backup folder '%s'",
                        backupPath
                    ),
                    e
                );
            }
        }
    }

    private static void restoreForUser(Path userRestorePath, Log log) {
        var graphsPath = userRestorePath.resolve(GRAPHS_DIR);
        var modelsPath = userRestorePath.resolve(MODELS_DIR);
        DIRECTORY_IS_READABLE.validate(graphsPath);
        DIRECTORY_IS_READABLE.validate(modelsPath);

        restoreGraphs(graphsPath, log);
        restoreModels(modelsPath, log);
    }

    private static void createNewBackupFromRestore(Path backupPath, Iterable<Path> userPaths) throws IOException {
        var backupName = formatWithLocale("backup-%s-restored", UUID.randomUUID());
        var backupRoot = backupPath.resolve(backupName);
        Files.createDirectories(backupRoot);
        for (Path userPath : userPaths) {
            Files.move(userPath, backupRoot.resolve(userPath.getFileName()));
        }
    }

    private static List<BackupResult> backupGraphStores(
        Path backupRoot,
        Consumer<GraphStoreCatalog.GraphStoreWithUserNameAndConfig> onSuccess,
        Log log,
        AllocationTracker allocationTracker
    ) {
        return GraphStoreCatalog.getAllGraphStores()
            .flatMap(store -> {
                var username = store.userName();
                try {
                    var config = ImmutableGraphStoreToFileExporterConfig
                        .builder()
                        .includeMetaData(true)
                        .exportName(store.config().graphName())
                        .username(username)
                        .build();

                    var backupDir = backupRoot.resolve(username).resolve(GRAPHS_DIR);
                    var backupPath = GraphStoreExporterUtil.getExportPath(backupDir, config);

                    var timer = ProgressTimer.start();
                    GraphStoreExporterUtil.runGraphStoreExportToCsv(
                        store.graphStore(),
                        backupPath,
                        config,
                        log,
                        allocationTracker
                    );
                    timer.stop();

                    onSuccess.accept(store);

                    return Stream.of(BackupResult.successfulGraph(username, backupPath, timer));
                } catch (Exception e) {
                    log.error(
                        formatWithLocale(
                            "Persisting graph '%s' for user '%s' failed",
                            store.config().graphName(),
                            username
                        ),
                        e
                    );
                    return Stream.of(BackupResult.failedGraph(username));
                }
            }).collect(Collectors.toList());
    }

    private static List<BackupResult> backupModels(
        Path backupRoot,
        Consumer<Model<?, ?>> onSuccess,
        Log log
    ) {
        return ModelCatalog.getAllModels().flatMap(model -> {
            var username = model.creator();
            try {
                var backupDir = backupRoot.resolve(username).resolve(MODELS_DIR);
                var modelPath = backupDir.resolve(UUID.randomUUID().toString());
                DIRECTORY_IS_WRITABLE.validate(modelPath);

                var timer = ProgressTimer.start();
                ModelToFileExporter.toFile(modelPath, model);
                timer.stop();

                onSuccess.accept(model);

                return Stream.of(BackupResult.successfulModel(username, modelPath, timer));
            } catch (Exception e) {
                log.error(
                    formatWithLocale(
                        "Persisting model '%s' for user '%s' failed",
                        model.name(),
                        username
                    ),
                    e
                );
                return Stream.of(BackupResult.failedModel(username));
            }
        }).collect(Collectors.toList());
    }

    private static void restoreGraphs(Path storePath, Log log) {
        subdirectories(storePath, log).forEach(path -> restoreGraph(path, log));
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
        subdirectories(storePath, log).forEach(path -> restoreModel(path, log));
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

    private static Stream<Path> subdirectories(Path path, Log log) {
        try {
            return Files.list(path)
                .filter(not(dir -> ExceptionUtil.supply(() -> Files.isHidden(dir))))
                .peek(DIRECTORY_IS_READABLE::validate);
        } catch (IOException e) {
            log.error(
                formatWithLocale("Could not traverse subdirectories of '%s'", path),
                e
            );
            return Stream.empty();
        }
    }

    private BackupAndRestore() {}
}
