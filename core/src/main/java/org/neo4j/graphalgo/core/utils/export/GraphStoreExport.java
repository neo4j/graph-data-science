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

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;
import org.neo4j.common.Validator;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.RelationshipType;
import org.neo4j.graphalgo.api.GraphStore;
import org.neo4j.graphalgo.api.NodeMapping;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.compat.Neo4jProxy;
import org.neo4j.graphalgo.core.Settings;
import org.neo4j.graphalgo.core.huge.AdjacencyList;
import org.neo4j.graphalgo.core.huge.AdjacencyOffsets;
import org.neo4j.graphalgo.core.huge.HugeGraph;
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
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.internal.NullLogService;
import org.neo4j.logging.internal.StoreLogService;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.eclipse.collections.impl.tuple.Tuples.pair;
import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;
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

            Input input = Neo4jProxy.batchInputFrom(new GraphStoreInput(
                NodeStore.of(graphStore),
                RelationshipStore.of(graphStore, config.defaultRelationshipType()),
                config.batchSize()
            ));

            var metaDataPath = databaseLayout.metadataStore().toPath();
            var dbExists = Files.exists(metaDataPath) && Files.isReadable(metaDataPath);
            if (dbExists) {
                throw new IllegalArgumentException(formatWithLocale(
                    "The database [%s] already exists. The graph export procedure can only create new databases.",
                    config.dbName()
                ));
            }

            var importer = Neo4jProxy.instantiateBatchImporter(
                BatchImporterFactory.withHighestPriority(),
                databaseLayout,
                fs,
                null, // no external page cache
                PageCacheTracer.NULL,
                importConfig,
                logService,
                ExecutionMonitors.invisible(),
                AdditionalInitialIds.EMPTY,
                databaseConfig,
                RecordFormatSelector.selectForConfig(databaseConfig, logService.getInternalLogProvider()),
                ImportLogic.NO_MONITOR,
                jobScheduler,
                Collector.EMPTY
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

        final HugeIntArray labelCounts;

        final NodeMapping nodeLabels;

        final Map<String, Map<String, NodeProperties>> nodeProperties;

        private final Set<NodeLabel> availableNodeLabels;

        NodeStore(
            long nodeCount,
            HugeIntArray labelCounts,
            NodeMapping nodeLabels,
            Map<String, Map<String, NodeProperties>> nodeProperties
        ) {
            this.nodeCount = nodeCount;
            this.labelCounts = labelCounts;
            this.nodeLabels = nodeLabels;
            this.nodeProperties = nodeProperties;
            this.availableNodeLabels = nodeLabels != null ? nodeLabels.availableNodeLabels() : null;
        }

        boolean hasLabels() {
            return nodeLabels != null;
        }

        boolean hasProperties() {
            return nodeProperties != null;
        }

        int labelCount() {
            return !hasLabels() ? 0 : nodeLabels.availableNodeLabels().size();
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
            for (var nodeLabel : availableNodeLabels) {
                if (nodeLabels.hasLabel(nodeId, nodeLabel)) {
                    labels[i++] = nodeLabel.name;
                }
            }

            return labels;
        }

        static NodeStore of(GraphStore graphStore) {
            HugeIntArray labelCounts = null;
            Map<String, Map<String, NodeProperties>> nodeProperties;

            var nodeLabels = graphStore.nodes();

            if (!nodeLabels.containsOnlyAllNodesLabel()) {
                labelCounts = HugeIntArray.newArray(graphStore.nodeCount(), AllocationTracker.EMPTY);
                labelCounts.setAll(i -> {
                    int labelCount = 0;
                    for (var nodeLabel : nodeLabels.availableNodeLabels()) {
                        if (nodeLabels.hasLabel(i, nodeLabel)) {
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
                        propertyKey -> graphStore.nodePropertyValues(entry.getKey(), propertyKey)
                    ))
                ));
            }
            return new NodeStore(
                graphStore.nodeCount(),
                labelCounts,
                nodeLabels.containsOnlyAllNodesLabel() ? null : nodeLabels,
                nodeProperties
            );
        }
    }

    static class RelationshipStore {

        final long nodeCount;
        final long relationshipCount;

        final Map<RelationshipType, CompositeRelationshipIterator> relationshipIterators;

        RelationshipStore(
            long nodeCount,
            long relationshipCount,
            Map<RelationshipType, CompositeRelationshipIterator> relationshipIterators
        ) {
            this.nodeCount = nodeCount;
            this.relationshipCount = relationshipCount;
            this.relationshipIterators = relationshipIterators;
        }

        long propertyCount() {
            return relationshipIterators.values().stream().mapToInt(CompositeRelationshipIterator::propertyCount).sum();
        }

        RelationshipStore concurrentCopy() {
            return new RelationshipStore(
                nodeCount,
                relationshipCount,
                relationshipIterators.entrySet().stream().collect(Collectors.toMap(
                    Map.Entry::getKey,
                    entry -> entry.getValue().concurrentCopy()
                ))
            );
        }

        static RelationshipStore of(GraphStore graphStore, String defaultRelationshipType) {
            Map<RelationshipType, HugeGraph.TopologyCSR> topologies = new HashMap<>();
            Map<RelationshipType, Map<String, HugeGraph.PropertyCSR>> properties = new HashMap<>();

            graphStore.relationshipTypes().stream()
                // extract (relationshipType, propertyKey) tuples
                .flatMap(relType -> graphStore.relationshipPropertyKeys(relType).isEmpty()
                    ? Stream.of(pair(relType, Optional.<String>empty()))
                    : graphStore
                        .relationshipPropertyKeys(relType)
                        .stream()
                        .map(propertyKey -> pair(relType, Optional.of(propertyKey))))
                // extract graph for relationship type and property
                .map(relTypeAndProperty -> pair(
                    relTypeAndProperty,
                    graphStore.getGraph(relTypeAndProperty.getOne(), relTypeAndProperty.getTwo())
                ))
                // extract Topology list and associated Properties lists
                .forEach(relTypeAndPropertyAndGraph -> {
                    var relationshipType = relTypeAndPropertyAndGraph.getOne().getOne();
                    var maybePropertyKey = relTypeAndPropertyAndGraph.getOne().getTwo();
                    var graph = relTypeAndPropertyAndGraph.getTwo();

                    topologies.computeIfAbsent(relationshipType, ignored -> ((HugeGraph) graph).relationships().topology());
                    maybePropertyKey.ifPresent(propertyKey -> properties
                        .computeIfAbsent(relationshipType, ignored -> new HashMap<>())
                        // .get() is safe, since we have a property key
                        .put(propertyKey, ((HugeGraph) graph).relationships().properties().get()));
                });

            Map<RelationshipType, CompositeRelationshipIterator> relationshipIterators = new HashMap<>();

            // for each relationship type, merge its Topology list and all associated Property lists
            topologies.forEach((relationshipType, topology) -> {
                var adjacencyList = (AdjacencyList) topology.list();
                var adjacencyOffsets = (AdjacencyOffsets) topology.offsets();

                var propertyLists = properties.getOrDefault(relationshipType, Map.of())
                    .entrySet()
                    .stream()
                    .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> entry.getValue().list()
                    ));

                var propertyOffsets = properties.getOrDefault(relationshipType, Map.of())
                    .entrySet()
                    .stream()
                    .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> entry.getValue().offsets()
                    ));

                // iff relationshipType is '*', change it the given default
                var outputRelationshipType = relationshipType.equals(RelationshipType.ALL_RELATIONSHIPS)
                    ? RelationshipType.of(defaultRelationshipType)
                    : relationshipType;

                relationshipIterators.put(
                    outputRelationshipType,
                    new CompositeRelationshipIterator(adjacencyList, adjacencyOffsets, propertyLists, propertyOffsets)
                );
            });

            return new RelationshipStore(
                graphStore.nodeCount(),
                graphStore.relationshipCount(),
                relationshipIterators
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
