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

import com.carrotsearch.hppc.ObjectLongMap;
import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.impl.tuple.Tuples;
import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.NodeProjections;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.PropertyMappings;
import org.neo4j.graphalgo.RelationshipProjections;
import org.neo4j.graphalgo.RelationshipType;
import org.neo4j.graphalgo.api.CSRGraphStoreFactory;
import org.neo4j.graphalgo.api.GraphLoaderContext;
import org.neo4j.graphalgo.config.GraphCreateFromStoreConfig;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.GraphDimensionsStoreReader;
import org.neo4j.graphalgo.core.SecureTransaction;
import org.neo4j.graphalgo.core.huge.HugeGraph;
import org.neo4j.graphalgo.core.huge.TransientAdjacencyList;
import org.neo4j.graphalgo.core.huge.TransientAdjacencyOffsets;
import org.neo4j.graphalgo.core.loading.nodeproperties.NodePropertiesFromStoreBuilder;
import org.neo4j.graphalgo.core.utils.BatchingProgressLogger;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexValueCapability;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.values.storable.ValueCategory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toMap;
import static org.neo4j.graphalgo.core.GraphDimensions.ANY_LABEL;
import static org.neo4j.graphalgo.core.GraphDimensionsValidation.validate;
import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

public final class NativeFactory extends CSRGraphStoreFactory<GraphCreateFromStoreConfig> {

    private final GraphCreateFromStoreConfig storeConfig;

    public NativeFactory(
        GraphCreateFromStoreConfig graphCreateConfig,
        GraphLoaderContext loadingContext
    ) {
        this(
            graphCreateConfig,
            loadingContext,
            new GraphDimensionsStoreReader(loadingContext.transaction(), graphCreateConfig).call()
        );
    }

    public NativeFactory(
        GraphCreateFromStoreConfig graphCreateConfig,
        GraphLoaderContext loadingContext,
        GraphDimensions graphDimensions
    ) {
        super(graphCreateConfig, loadingContext, graphDimensions);
        this.storeConfig = graphCreateConfig;
    }

    @Override
    public MemoryEstimation memoryEstimation() {
        return getMemoryEstimation(storeConfig.nodeProjections(), storeConfig.relationshipProjections());
    }

    public static MemoryEstimation getMemoryEstimation(
        NodeProjections nodeProjections,
        RelationshipProjections relationshipProjections
    ) {
        MemoryEstimations.Builder builder = MemoryEstimations.builder(HugeGraph.class);

        // node information
        builder.add("nodeIdMap", IdMap.memoryEstimation());

        // nodeProperties
        nodeProjections.allProperties()
            .forEach(property -> builder.add(property, NodePropertiesFromStoreBuilder.memoryEstimation()));

        // relationships
        relationshipProjections.projections().forEach((relationshipType, relationshipProjection) -> {

            boolean undirected = relationshipProjection.orientation() == Orientation.UNDIRECTED;

            // adjacency list
            builder.add(
                formatWithLocale("adjacency list for '%s'", relationshipType),
                TransientAdjacencyList.compressedMemoryEstimation(relationshipType, undirected)
            );
            builder.add(
                formatWithLocale("adjacency offsets for '%s'", relationshipType),
                TransientAdjacencyOffsets.memoryEstimation()
            );
            // all properties per projection
            relationshipProjection.properties().mappings().forEach(resolvedPropertyMapping -> {
                builder.add(
                    formatWithLocale("property '%s.%s", relationshipType, resolvedPropertyMapping.propertyKey()),
                    TransientAdjacencyList.uncompressedMemoryEstimation(relationshipType, undirected)
                );
                builder.add(
                    formatWithLocale("property offset '%s.%s", relationshipType, resolvedPropertyMapping.propertyKey()),
                    TransientAdjacencyOffsets.memoryEstimation()
                );
            });
        });

        return builder.build();
    }

    @Override
    protected ProgressLogger initProgressLogger() {
        long relationshipCount = graphCreateConfig
            .relationshipProjections()
            .projections()
            .entrySet()
            .stream()
            .map(entry -> {
                Long relCount = entry.getKey().name.equals("*")
                    ? dimensions.relationshipCounts().values().stream().reduce(Long::sum).orElse(0L)
                    : dimensions.relationshipCounts().getOrDefault(entry.getKey(), 0L);

                return entry.getValue().orientation() == Orientation.UNDIRECTED
                    ? relCount * 2
                    : relCount;
            }).mapToLong(Long::longValue).sum();

        return new BatchingProgressLogger(
            loadingContext.log(),
            dimensions.nodeCount() + relationshipCount,
            TASK_LOADING,
            graphCreateConfig.readConcurrency()
        );
    }

    @Override
    public ImportResult<CSRGraphStore> build() {
        validate(dimensions, storeConfig);

        int concurrency = graphCreateConfig.readConcurrency();
        AllocationTracker tracker = loadingContext.tracker();
        IdsAndProperties nodes = loadNodes(concurrency);
        RelationshipImportResult relationships = loadRelationships(tracker, nodes, concurrency);
        CSRGraphStore graphStore = createGraphStore(nodes, relationships, tracker, dimensions);

        logLoadingSummary(graphStore, Optional.of(tracker));

        return ImportResult.of(dimensions, graphStore);
    }

    private IdsAndProperties loadNodes(int concurrency) {
        Map<NodeLabel, PropertyMappings> propertyMappingsByNodeLabel = graphCreateConfig
            .nodeProjections()
            .projections()
            .entrySet()
            .stream()
            .collect(toMap(
                Map.Entry::getKey,
                entry -> entry.getValue().properties()
            ));

        Map<NodeLabel, List<Pair<PropertyMapping, IndexDescriptor>>> indexedPropertyMappingsByNodeLabel = new HashMap<>();

        var transaction = loadingContext.transaction();
        Optional<SecureTransaction> closeTx = Optional.empty();
        KernelTransaction ktx;
        var topLevelKtx = transaction.topLevelKernelTransaction();
        if (topLevelKtx.isPresent()) {
            ktx = topLevelKtx.get();
        } else {
            closeTx = Optional.of(transaction.fork());
            ktx = closeTx.get().topLevelKernelTransaction().get();
        }
        var tx = ktx.internalTransaction();

        var nodeLabelMapping = dimensions.tokenNodeLabelMapping();
        if (nodeLabelMapping != null) {
            var schema = tx.schema();
            var schemaRead = ktx.schemaRead();

            var labelIds = StreamSupport.stream(nodeLabelMapping.keys().spliterator(), false);
            var nonAllLabelIds = labelIds.filter(labelId -> labelId.value != ANY_LABEL);

            var indexesForLabels = nonAllLabelIds.flatMap(label ->
                Iterators.stream(schemaRead.indexesGetForLabel(label.value)));

            var validIndexes = indexesForLabels.filter(id ->
                id != IndexDescriptor.NO_INDEX
                && id.getCapability().valueCapability(ValueCategory.NUMBER) == IndexValueCapability.YES
                && id.schema().getPropertyIds().length == 1
            );

            var onlineIndexes = validIndexes.filter(id -> {
                try {
                    // give the index a second the get online
                    schema.awaitIndexOnline(id.getName(), 1, TimeUnit.SECONDS);
                    return true;
                } catch (RuntimeException notOnline) {
                    // index not available, load via store scanning instead
                    return false;
                }
            });

            var indexPerLabelMappings = onlineIndexes.flatMap(id -> nodeLabelMapping
                .get(id.schema().getLabelId())
                .stream()
                .map(label -> Map.entry(label, id))
            );

            Map<NodeLabel, Map<Integer, IndexDescriptor>> indexLabelPropertyMappings = indexPerLabelMappings.collect(
                groupingBy(
                    Map.Entry::getKey,
                    toMap(id -> id.getValue().schema().getPropertyId(), Map.Entry::getValue)
                ));

            indexLabelPropertyMappings.forEach((nodeLabel, indexPerPropertyKeyId) -> {
                var propertyMappings = propertyMappingsByNodeLabel.get(nodeLabel);
                if (propertyMappings != null) {
                    var storeMappingsBuilder = PropertyMappings.builder();
                    var indexMappings = new ArrayList<Pair<PropertyMapping, IndexDescriptor>>();
                    propertyMappings.mappings().forEach(mapping -> {
                        var propertyKey = dimensions.nodePropertyTokens().get(mapping.neoPropertyKey());
                        if (propertyKey != null) {
                            var indexDescriptor = indexPerPropertyKeyId.get(propertyKey);
                            if (indexDescriptor != null) {
                                indexMappings.add(Tuples.pair(mapping, indexDescriptor));
                            } else {
                                storeMappingsBuilder.addMapping(mapping);
                            }
                        }
                    });
                    var storeMappings = storeMappingsBuilder.build();
                    if (storeMappings.hasMappings()) {
                        propertyMappingsByNodeLabel.put(nodeLabel, storeMappings);
                    } else {
                        propertyMappingsByNodeLabel.remove(nodeLabel);
                    }
                    if (!indexMappings.isEmpty()) {
                        indexedPropertyMappingsByNodeLabel.put(nodeLabel, indexMappings);
                    }
                }
            });
        }


        return new ScanningNodesImporter(
            graphCreateConfig,
            loadingContext,
            dimensions,
            progressLogger,
            concurrency,
            propertyMappingsByNodeLabel,
            indexedPropertyMappingsByNodeLabel
        ).call(loadingContext.log());
    }

    private RelationshipImportResult loadRelationships(
        AllocationTracker tracker,
        IdsAndProperties idsAndProperties,
        int concurrency
    ) {
        var pageSize = ImportSizing.of(concurrency, dimensions.nodeCount()).pageSize();
        Map<RelationshipType, RelationshipsBuilder> allBuilders = graphCreateConfig
            .relationshipProjections()
            .projections()
            .entrySet()
            .stream()
            .collect(toMap(
                Map.Entry::getKey,
                projectionEntry -> new RelationshipsBuilder(
                    projectionEntry.getValue(),
                    TransientAdjacencyListBuilder.builderFactory(tracker),
                    TransientAdjacencyOffsets.forPageSize(pageSize)
                )
            ));

        ObjectLongMap<RelationshipType> relationshipCounts = new ScanningRelationshipsImporter(
            graphCreateConfig,
            loadingContext,
            dimensions,
            progressLogger,
            idsAndProperties.idMap,
            allBuilders,
            concurrency
        ).call(loadingContext.log());

        return RelationshipImportResult.of(allBuilders, relationshipCounts, dimensions);
    }
}
