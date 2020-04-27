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

import com.carrotsearch.hppc.BitSet;
import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.impl.tuple.Tuples;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;
import org.neo4j.batchinsert.internal.TransactionLogsInitializer;
import org.neo4j.common.Validator;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphalgo.RelationshipType;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.api.RelationshipIterator;
import org.neo4j.graphalgo.core.Settings;
import org.neo4j.graphalgo.core.loading.GraphStore;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeIntArray;
import org.neo4j.internal.batchimport.AdditionalInitialIds;
import org.neo4j.internal.batchimport.BatchImporterFactory;
import org.neo4j.internal.batchimport.Configuration;
import org.neo4j.internal.batchimport.ImportLogic;
import org.neo4j.internal.batchimport.input.Collector;
import org.neo4j.internal.batchimport.input.Input;
import org.neo4j.internal.batchimport.staging.ExecutionMonitors;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.internal.NullLogService;
import org.neo4j.logging.internal.StoreLogService;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.neo4j.io.ByteUnit.mebiBytes;
import static org.neo4j.kernel.impl.scheduler.JobSchedulerFactory.createScheduler;

public class GraphStoreExport {

    private final GraphStore graphStore;

    private final File neo4jHome;

    private final GraphStoreExportConfig config;

    public GraphStoreExport(GraphStore graphStore, File neo4jHome, GraphStoreExportConfig config) {
        this.graphStore = graphStore;
        this.neo4jHome = neo4jHome;
        this.config = config;
    }

    public void run() {
        run(false);
    }

    /**
     * Runs with default configuration geared towards
     * unit/integration test environments, for example,
     * lower default buffer sizes.
     */
    @TestOnly
    public void runFromTests() {
        run(true);
    }

    private void run(boolean defaultSettingsSuitableForTests) {
        DIRECTORY_IS_WRITABLE.validate(neo4jHome);
        var databaseConfig = Config.defaults(GraphDatabaseSettings.neo4j_home, neo4jHome.toPath());
        var databaseLayout = Neo4jLayout.of(databaseConfig).databaseLayout(config.dbName());
        var importConfig = getImportConfig(defaultSettingsSuitableForTests);

        var lifeSupport = new LifeSupport();

        try (FileSystemAbstraction fs = new DefaultFileSystemAbstraction()) {
            var logService = config.enableDebugLog()
                ? lifeSupport.add(StoreLogService.withInternalLog(databaseConfig.get(Settings.storeInternalLogPath()).toFile()).build(fs))
                : NullLogService.getInstance();
            var jobScheduler = lifeSupport.add(createScheduler());

            lifeSupport.start();

            Input input = new GraphStoreInput(
                NodeStore.of(graphStore),
                RelationshipStore.of(graphStore),
                config.batchSize()
            );

            var importer = BatchImporterFactory.withHighestPriority().instantiate(
                databaseLayout,
                fs,
                null, // no external page cache
                importConfig,
                logService,
                ExecutionMonitors.invisible(),
                AdditionalInitialIds.EMPTY,
                databaseConfig,
                RecordFormatSelector.selectForConfig(databaseConfig, logService.getInternalLogProvider()),
                ImportLogic.NO_MONITOR,
                jobScheduler,
                Collector.EMPTY,
                TransactionLogsInitializer.INSTANCE
            );
            importer.doImport(input);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            lifeSupport.shutdown();
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

    static class NodeStore {

        static final String[] EMPTY_LABELS = new String[0];

        final long nodeCount;

        @Nullable
        final HugeIntArray labelCounts;

        @Nullable
        final Map<String, BitSet> nodeLabels;

        @Nullable
        final Map<String, Map<String, NodeProperties>> nodeProperties;

        NodeStore(
            long nodeCount,
            @Nullable HugeIntArray labelCounts,
            @Nullable Map<String, BitSet> nodeLabels,
            @Nullable Map<String, Map<String, NodeProperties>> nodeProperties
        ) {
            this.nodeCount = nodeCount;
            this.labelCounts = labelCounts;
            this.nodeLabels = nodeLabels;
            this.nodeProperties = nodeProperties;
        }

        boolean hasLabels() {
            return nodeLabels != null;
        }

        boolean hasProperties() {
            return nodeProperties != null;
        }

        int labelCount() {
            return !hasLabels() ? 0 : nodeLabels.size();
        }

        int propertyCount() {
            if (nodeProperties == null) {
                return 0;
            } else {
                return nodeProperties.values().stream().mapToInt(Map::size).sum();
            }
        }

        String[] labels(long nodeId) {
            int labelCount = labelCounts.get(nodeId);
            if (labelCount == 0) {
                return EMPTY_LABELS;
            }
            String[] labels = new String[labelCount];

            int i = 0;
            for (var nodeLabelToBitSet : nodeLabels.entrySet()) {
                if (nodeLabelToBitSet.getValue().get(nodeId)) {
                    labels[i++] = nodeLabelToBitSet.getKey();
                }
            }

            return labels;
        }

        static NodeStore of(GraphStore graphStore) {
            HugeIntArray labelCounts = null;
            Map<String, BitSet> nodeLabels = null;
            Map<String, Map<String, NodeProperties>> nodeProperties;

            if (graphStore.nodes().hasLabelInformation()) {
                var nodeLabelBitSetMap = graphStore.nodes().maybeLabelInformation().get();

                nodeLabels = nodeLabelBitSetMap.entrySet().stream()
                    .collect(Collectors.toMap(
                        entry -> entry.getKey().name,
                        Map.Entry::getValue
                    ));

                labelCounts = HugeIntArray.newArray(graphStore.nodeCount(), AllocationTracker.EMPTY);
                labelCounts.setAll(i -> {
                    int labelCount = 0;
                    for (BitSet value : nodeLabelBitSetMap.values()) {
                        if (value.get(i)) {
                            labelCount++;
                        }
                    }
                    return labelCount;
                });
            }

            if (graphStore.nodePropertyCount() == 0) {
                nodeProperties = null;
            } else {
                nodeProperties = graphStore.nodePropertyKeys().entrySet().stream().collect(Collectors.toMap(
                    entry -> entry.getKey().name,
                    entry -> entry.getValue().stream().collect(Collectors.toMap(
                        propertyKey -> propertyKey,
                        propertyKey -> graphStore.nodeProperty(entry.getKey(), propertyKey).values()
                    ))
                ));
            }
            return new NodeStore(graphStore.nodeCount(), labelCounts, nodeLabels, nodeProperties);
        }
    }

    static class RelationshipStore {

        final long nodeCount;
        final long relationshipCount;

        final Map<String, RelationshipIterator> relationships;

        final Map<String, String> relationshipPropertyKeys;

        final String[] relTypes;

        final String[] propertyKeys;

        RelationshipStore(
            long nodeCount,
            long relationshipCount,
            Map<String, RelationshipIterator> relationships,
            Map<String, String> relationshipPropertyKeys
        ) {
            this.nodeCount = nodeCount;
            this.relationshipCount = relationshipCount;
            this.relationships = relationships;
            this.relationshipPropertyKeys = relationshipPropertyKeys;

            this.relTypes = relationships.keySet().toArray(new String[0]);
            this.propertyKeys = Arrays.stream(relTypes).map(relationshipPropertyKeys::get).toArray(String[]::new);
        }

        int propertyCount() {
            return relationshipPropertyKeys.size();
        }

        RelationshipStore concurrentCopy() {
            return new RelationshipStore(
                nodeCount,
                relationshipCount,
                relationships.entrySet().stream().collect(Collectors.toMap(
                    Map.Entry::getKey,
                    entry -> entry.getValue().concurrentCopy()
                )),
                relationshipPropertyKeys
            );
        }

        static RelationshipStore of(GraphStore graphStore) {
            Map<Pair<RelationshipType, Optional<String>>, Graph> graphs = graphStore
                .relationshipTypes()
                .stream()
                .flatMap(relType -> {
                    Set<String> relProperties = graphStore.relationshipPropertyKeys(relType);
                    if (relProperties.isEmpty()) {
                        return Stream.of(Tuples.pair(relType, Optional.<String>empty()));
                    } else {
                        return relProperties
                            .stream()
                            .map(propertyKey -> Tuples.pair(relType, Optional.of(propertyKey)));
                    }
                })
                .collect(Collectors.toMap(
                    relTypeAndProperty -> relTypeAndProperty,
                    relTypeAndProperty -> graphStore.getGraph(relTypeAndProperty.getOne(), relTypeAndProperty.getTwo())
                ));

            Map<String, RelationshipIterator> relationships = graphs.entrySet().stream().collect(Collectors.toMap(
                entry -> entry.getKey().getOne().name,
                Map.Entry::getValue
            ));

            var relationshipPropertyKeys = graphs.keySet().stream()
                .filter(pair -> pair.getTwo().isPresent())
                .collect(Collectors.toMap(
                    entry -> entry.getOne().name,
                    entry -> entry.getTwo().get()
                ));

            return new RelationshipStore(
                graphStore.nodeCount(),
                graphStore.relationshipCount(),
                relationships,
                relationshipPropertyKeys
            );
        }
    }

    private static final Validator<File> DIRECTORY_IS_WRITABLE = value -> {
        if (value.mkdirs()) {   // It's OK, we created the directory right now, which means we have write access to it
            return;
        }

        var test = new File(value, "_______test___");
        try {
            test.createNewFile();
        } catch (IOException e) {
            throw new IllegalArgumentException("Directory '" + value + "' not writable: " + e.getMessage());
        } finally {
            test.delete();
        }
    };
}
