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
package org.neo4j.graphalgo.core.loading;

import com.carrotsearch.hppc.IntObjectMap;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.api.GraphLoaderContext;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.config.GraphCreateFromStoreConfig;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.concurrency.ParallelUtil;
import org.neo4j.graphalgo.core.loading.IndexPropertyMappings.LoadablePropertyMappings;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.paged.HugeAtomicBitSet;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArrayBuilder;

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
import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;


final class ScanningNodesImporter extends ScanningRecordsImporter<NodeReference, IdsAndProperties> {

    private final GraphCreateFromStoreConfig graphCreateConfig;
    private final ProgressLogger progressLogger;
    private final TerminationFlag terminationFlag;
    private final LoadablePropertyMappings properties;

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
        LoadablePropertyMappings properties
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
        this.properties = properties;
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

        if (!properties.indexedProperties().isEmpty()) {
            importPropertiesFromIndex(hugeIdMap, nodeProperties);
        }

        return IdsAndProperties.of(hugeIdMap, nodeProperties);
    }

    private void importPropertiesFromIndex(
        IdMap hugeIdMap,
        Map<NodeLabel, Map<PropertyMapping, NodeProperties>> nodeProperties
    ) {
        long indexStart = System.nanoTime();
        progressLogger.logMessage("Property Index Scan :: Start");

        var indexScanningImporters = properties.indexedProperties()
            .entrySet()
            .stream()
            .flatMap(labelAndProperties -> labelAndProperties
                .getValue()
                .mappings()
                .stream()
                .map(mappingAndIndex -> new IndexedNodePropertyImporter(
                    transaction,
                    labelAndProperties.getKey(),
                    mappingAndIndex.property(),
                    mappingAndIndex.index(),
                    hugeIdMap,
                    progressLogger,
                    terminationFlag,
                    tracker
                ))
            ).collect(Collectors.toList());

        var expectedProperties = ((long) indexScanningImporters.size()) * hugeIdMap.nodeCount();
        var remainingVolume = progressLogger.reset(expectedProperties);

        long recordsImported = 0L;
        ParallelUtil.run(indexScanningImporters, threadPool);
        for (IndexedNodePropertyImporter propertyImporter : indexScanningImporters) {
            var nodeLabel = propertyImporter.nodeLabel();
            var storeProperties = nodeProperties.computeIfAbsent(nodeLabel, ignore -> new HashMap<>());
            storeProperties.put(propertyImporter.mapping(), propertyImporter.build());
            recordsImported += propertyImporter.imported();
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

        progressLogger.logMessage(formatWithLocale(
            "Property Index Scan: Imported %,d properties; took %.3f s, %,.2f Properties/s",
            recordsImported,
            tookInSeconds,
            recordsPerSecond
        ));
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
        var propertyMappingsByLabel = properties.storedProperties();
        boolean loadProperties = propertyMappingsByLabel
            .values()
            .stream()
            .anyMatch(mappings -> mappings.numberOfMappings() > 0);

        if (loadProperties) {
            return NativeNodePropertyImporter
                .builder()
                .nodeCount(nodeCount)
                .dimensions(dimensions)
                .propertyMappings(propertyMappingsByLabel)
                .tracker(tracker)
                .build();
        } else {
            return null;
        }
    }
}
