/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.core.utils.export;

import de.siegmar.fastcsv.writer.CsvAppender;
import de.siegmar.fastcsv.writer.CsvWriter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;
import org.neo4j.common.Validator;
import org.neo4j.configuration.Config;
import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.api.GraphStore;
import org.neo4j.graphalgo.compat.Neo4jProxy;
import org.neo4j.graphalgo.core.Settings;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.internal.batchimport.Configuration;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.internal.LogService;
import org.neo4j.logging.internal.NullLogService;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

import static org.neo4j.io.ByteUnit.mebiBytes;

public class GraphStoreExportToCSV {

    private final GraphStore graphStore;

    private final Path neo4jHome;

    private final GraphStoreExportConfig config;

    private final FileSystemAbstraction fs;

    public GraphStoreExportToCSV(
        GraphStore graphStore,
        GraphDatabaseAPI api,
        GraphStoreExportConfig config
    ) {
        this.graphStore = graphStore;
        this.neo4jHome = Neo4jProxy.homeDirectory(api.databaseLayout());
        this.config = config;
        this.fs = api.getDependencyResolver().resolveDependency(FileSystemAbstraction.class);
    }

    public void run(AllocationTracker tracker) {
        run(false, tracker);
    }

    /**
     * Runs with default configuration geared towards
     * unit/integration test environments, for example,
     * lower default buffer sizes.
     */
    @TestOnly
    public void runFromTests() {
        run(true, AllocationTracker.empty());
    }

    private void run(boolean defaultSettingsSuitableForTests, AllocationTracker tracker) {
        DIRECTORY_IS_WRITABLE.validate(neo4jHome);
        var databaseConfig = Config.defaults(Settings.neo4jHome(), neo4jHome);
        var databaseLayout = Neo4jLayout.of(databaseConfig).databaseLayout(config.dbName());
        var importConfig = getImportConfig(defaultSettingsSuitableForTests);

        try {
            LogService logService;
            if (config.enableDebugLog()) {
                var storeInternalLogPath = databaseConfig.get(Settings.storeInternalLogPath());
            } else {
                logService = NullLogService.getInstance();
            }

            // write node file
            CsvWriter csvWriter = new CsvWriter();

            File nodeFile = new File("nodes.csv");
            try (CsvAppender csvAppender = csvWriter.append(nodeFile, StandardCharsets.UTF_8)) {

                var nodeCsvColumns = Arrays.asList(":ID", ":LABELS");
                Set<String> nodeProperties = graphStore
                    .nodePropertyKeys()
                    .values()
                    .stream()
                    .reduce((set, otherSet) -> {
                        set.addAll(otherSet);
                        return set;
                    }).orElseGet(Collections::emptySet);

                nodeCsvColumns.addAll(nodeProperties);

                // header
                csvAppender.appendLine(String.join(",", nodeCsvColumns));

                for (long id = 0; id < graphStore.nodeCount(); id++) {
                    csvAppender.appendField(Long.toString(id));
                    csvAppender.appendField(graphStore.nodes().nodeLabels(id).stream().map(NodeLabel::name).collect(
                        Collectors.joining(";")));

                    for (var propName : nodeProperties) {
                        csvAppender.appendField(graphStore.nodePropertyValues(propName).value(id).prettyPrint());
                    }

                    csvAppender.endLine();
                }

                csvAppender.flush();
            }
            // write relationship files
            // TODO write rest
        } catch (IOException ioException) {
            throw new UncheckedIOException(ioException);
        }
    }

    @NotNull
    private Configuration getImportConfig(boolean defaultSettingsSuitableForTests) {
        return new Configuration() {
            @Override
            public int maxNumberOfProcessors() {
                return config.writeConcurrency();
            }

            @Override
            public long pageCacheMemory() {
                return defaultSettingsSuitableForTests ? mebiBytes(8) : Configuration.super.pageCacheMemory();
            }

            @Override
            public boolean highIO() {
                return false;
            }
        };
    }

    private static final Validator<Path> DIRECTORY_IS_WRITABLE = value -> {
        try {
            Files.createDirectories(value);
            if (!Files.isDirectory(value)) {
                throw new IllegalArgumentException("'" + value + "' is not a directory");
            }
            if (!Files.isWritable(value)) {
                throw new IllegalArgumentException("Directory '" + value + "' not writable");
            }
        } catch (IOException e) {
            throw new IllegalArgumentException("Directory '" + value + "' not writable: ", e);
        }
    };
}
