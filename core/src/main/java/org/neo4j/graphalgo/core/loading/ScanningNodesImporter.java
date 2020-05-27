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
package org.neo4j.graphalgo.core.loading;

import com.carrotsearch.hppc.IntObjectMap;
import org.eclipse.collections.api.tuple.Pair;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.PropertyMappings;
import org.neo4j.graphalgo.api.GraphLoaderContext;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.config.GraphCreateFromStoreConfig;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.SecureTransaction;
import org.neo4j.graphalgo.core.concurrency.ParallelUtil;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.StatementAction;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.paged.HugeAtomicBitSet;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArrayBuilder;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.kernel.api.KernelTransaction;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.neo4j.graphalgo.core.GraphDimensions.ANY_LABEL;


final class ScanningNodesImporter extends ScanningRecordsImporter<NodeReference, IdsAndProperties> {

    private final GraphCreateFromStoreConfig graphCreateConfig;
    private final ProgressLogger progressLogger;
    private final TerminationFlag terminationFlag;
    private final Map<NodeLabel, PropertyMappings> propertyMappingsByNodeLabel;
    private final Map<NodeLabel, List<Pair<PropertyMapping, IndexDescriptor>>> indexPropertyMappingsByNodeLabel;

    @Nullable
    private NativeNodePropertyImporter nodePropertyImporter;
    private HugeLongArrayBuilder idMapBuilder;
    private Map<NodeLabel, HugeAtomicBitSet> nodeLabelBitSetMapping;

    ScanningNodesImporter(
        GraphCreateFromStoreConfig graphCreateConfig,
        GraphLoaderContext loadingContext,
        GraphDimensions dimensions,
        ProgressLogger progressLogger,
        int concurrency,
        Map<NodeLabel, PropertyMappings> propertyMappingsByNodeLabel,
        Map<NodeLabel, List<Pair<PropertyMapping, IndexDescriptor>>> indexPropertyMappingsByNodeLabel
    ) {
        super(
            scannerFactory(dimensions),
            "Node",
            loadingContext,
            dimensions,
            concurrency
        );
        this.graphCreateConfig = graphCreateConfig;
        this.progressLogger = progressLogger;
        this.terminationFlag = loadingContext.terminationFlag();
        this.propertyMappingsByNodeLabel = propertyMappingsByNodeLabel;
        this.indexPropertyMappingsByNodeLabel = indexPropertyMappingsByNodeLabel;
    }

    private static StoreScanner.Factory<NodeReference> scannerFactory(
        GraphDimensions dimensions
    ) {
        var tokenNodeLabelMapping = dimensions.tokenNodeLabelMapping();
        assert tokenNodeLabelMapping != null : "Only null in Cypher loader";

        int[] labelIds = tokenNodeLabelMapping.keys().toArray();
        return NodeScannerFactory.create(labelIds);
    }

    @Override
    public InternalImporter.CreateScanner creator(
        long nodeCount,
        ImportSizing sizing,
        StoreScanner<NodeReference> scanner
    ) {
        idMapBuilder = HugeLongArrayBuilder.of(nodeCount, tracker);

        IntObjectMap<List<NodeLabel>> labelTokenNodeLabelMapping = dimensions.tokenNodeLabelMapping();

        nodeLabelBitSetMapping =
            graphCreateConfig.nodeProjections().allProjections().size() == 1
            && labelTokenNodeLabelMapping.containsKey(ANY_LABEL)
                ? Collections.emptyMap()
                : initializeLabelBitSets(nodeCount, labelTokenNodeLabelMapping);

        nodePropertyImporter = initializeNodePropertyImporter(nodeCount);

        return NodesScanner.of(
            transaction,
            scanner,
            dimensions.nodeLabelTokens(),
            progressLogger,
            new HugeNodeImporter(
                idMapBuilder,
                nodeLabelBitSetMapping,
                labelTokenNodeLabelMapping,
                tracker
            ),
            nodePropertyImporter,
            terminationFlag
        );
    }

    @Override
    public IdsAndProperties build() {
        IdMap hugeIdMap = IdMapBuilder.build(
            idMapBuilder,
            nodeLabelBitSetMapping,
            dimensions.highestNeoId(),
            concurrency,
            tracker
        );

        Map<NodeLabel, Map<PropertyMapping, NodeProperties>> nodeProperties = nodePropertyImporter == null
            ? new HashMap<>()
            : nodePropertyImporter.result();

        if (!indexPropertyMappingsByNodeLabel.isEmpty()) {
            importPropertiesFromIndex(hugeIdMap, nodeProperties);
        }

        return IdsAndProperties.of(hugeIdMap, nodeProperties);
    }

    private void importPropertiesFromIndex(
        IdMap hugeIdMap,
        Map<NodeLabel, Map<PropertyMapping, NodeProperties>> nodeProperties
    ) {
        class IndexPropertyImporter extends StatementAction {
            private final NodeLabel nodeLabel;
            private final PropertyMapping mapping;
            private final IndexDescriptor index;
            private final int propertyId;
            private final NodePropertiesBuilder propertiesBuilder;
            private long imported;
            private long logged;

            IndexPropertyImporter(
                SecureTransaction tx,
                NodeLabel nodeLabel,
                PropertyMapping mapping,
                IndexDescriptor index
            ) {
                super(tx);
                this.nodeLabel = nodeLabel;
                this.mapping = mapping;
                this.index = index;
                propertyId = index.schema().getPropertyId();
                propertiesBuilder = NodePropertiesBuilder.of(
                    hugeIdMap.nodeCount(),
                    tracker,
                    mapping.defaultValue(),
                    propertyId,
                    mapping.propertyKey(),
                    concurrency
                );
            }

            @Override
            public String threadName() {
                return "index-scan-" + index.getName();
            }

            @Override
            public void accept(KernelTransaction ktx) throws Exception {
                var read = ktx.dataRead();
                try (var nvic = ktx.cursors().allocateNodeValueIndexCursor()) {
                    var indexReadSession = read.indexReadSession(index);
                    read.nodeIndexScan(indexReadSession, nvic, IndexOrder.NONE, true);
                    while (nvic.next()) {
                        if (nvic.hasValue()) {
                            var node = nvic.nodeReference();
                            var numberOfProperties = nvic.numberOfProperties();
                            for (int i = 0; i < numberOfProperties; i++) {
                                var propertyKey = nvic.propertyKey(i);
                                if (propertyId == propertyKey) {
                                    var propertyValue = nvic.propertyValue(i);
                                    var value = ReadHelper.extractValue(propertyValue, mapping.defaultValue());
                                    var nodeId = hugeIdMap.toMappedNodeId(node);
                                    propertiesBuilder.set(nodeId, value);
                                    imported += 1;
                                    if ((imported & 0x1_FFFFL) == 0L) {
                                        progressLogger.logProgress(imported - logged);
                                        logged = imported;
                                        terminationFlag.assertRunning();
                                    }
                                }
                            }
                        }
                    }
                }
            }

            NodeProperties build() {
                return propertiesBuilder.build();
            }
        }

        long indexStart = System.nanoTime();

        var indexScanningPropertyImporter = indexPropertyMappingsByNodeLabel
            .entrySet()
            .stream()
            .flatMap(labelAndProperties -> {
                return labelAndProperties.getValue().stream().map(mappingAndIndex -> {
                    var mapping = mappingAndIndex.getOne();
                    var index = mappingAndIndex.getTwo();
                    return new IndexPropertyImporter(transaction, labelAndProperties.getKey(), mapping, index);
                });
            })
            .collect(Collectors.toList());

        var expectedProperties = ((long) indexScanningPropertyImporter.size()) * hugeIdMap.nodeCount();
        var remainingVolume = progressLogger.reset(expectedProperties);

        long recordsImported = 0L;
        ParallelUtil.run(indexScanningPropertyImporter, threadPool);
        for (IndexPropertyImporter propertyImporter : indexScanningPropertyImporter) {
            var nodeLabel = propertyImporter.nodeLabel;
            var storeProperties = nodeProperties.computeIfAbsent(nodeLabel, ignore -> new HashMap<>());
            storeProperties.put(propertyImporter.mapping, propertyImporter.build());
            recordsImported += propertyImporter.imported;
        }

        long tookNanos = System.nanoTime() - indexStart;
        BigInteger bigNanos = BigInteger.valueOf(tookNanos);
        double tookInSeconds = new BigDecimal(bigNanos)
            .divide(new BigDecimal(A_BILLION), 9, RoundingMode.CEILING)
            .doubleValue();
        double recordsPerSecond = new BigDecimal(A_BILLION)
            .multiply(BigDecimal.valueOf(recordsImported))
            .divide(new BigDecimal(bigNanos), 9, RoundingMode.CEILING)
            .doubleValue();

        progressLogger.getLog().info(
            "Property Index Scan: Imported %,d properties; took %.3f s, %,.2f Properties/s",
            recordsImported,
            tookInSeconds,
            recordsPerSecond
        );
        progressLogger.reset(remainingVolume);
    }

    @NotNull
    private Map<NodeLabel, HugeAtomicBitSet> initializeLabelBitSets(
        long nodeCount,
        IntObjectMap<List<NodeLabel>> labelTokenNodeLabelMapping
    ) {
        var nodeLabelBitSetMap = StreamSupport.stream(
            labelTokenNodeLabelMapping.values().spliterator(),
            false
        )
            .flatMap(cursor -> cursor.value.stream())
            .distinct()
            .collect(Collectors.toMap(
                nodeLabel -> nodeLabel,
                nodeLabel -> HugeAtomicBitSet.create(nodeCount, tracker))
            );

        // set the whole range for '*' projections
        for (NodeLabel starLabel : labelTokenNodeLabelMapping.getOrDefault(ANY_LABEL, Collections.emptyList())) {
            nodeLabelBitSetMap.get(starLabel).set(0, nodeCount);
        }

        return nodeLabelBitSetMap;
    }

    @Nullable
    private NativeNodePropertyImporter initializeNodePropertyImporter(long nodeCount) {
        boolean loadProperties = propertyMappingsByNodeLabel
            .values()
            .stream()
            .anyMatch(mappings -> mappings.numberOfMappings() > 0);

        if (loadProperties) {
            return NativeNodePropertyImporter
                .builder()
                .nodeCount(nodeCount)
                .dimensions(dimensions)
                .propertyMappings(propertyMappingsByNodeLabel)
                .tracker(tracker)
                .build();
        } else {
            return null;
        }
    }
}
