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
package org.neo4j.gds.core.loading;

import com.carrotsearch.hppc.IntObjectMap;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.PropertyMapping;
import org.neo4j.gds.PropertyMappings;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.config.GraphProjectFromStoreConfig;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.transaction.TransactionContext;
import org.neo4j.gds.utils.GdsFeatureToggles;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.kernel.api.SchemaReadCore;
import org.neo4j.internal.schema.IndexDescriptor;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toMap;
import static org.neo4j.gds.core.GraphDimensions.ANY_LABEL;

// TODO: should be named LoadablePropertyMappings
final class IndexPropertyMappings {

    static Map<NodeLabel, PropertyMappings> propertyMappings(GraphProjectFromStoreConfig graphProjectConfig) {
        return graphProjectConfig
            .nodeProjections()
            .projections()
            .entrySet()
            .stream()
            .collect(toMap(
                Map.Entry::getKey,
                entry -> entry
                    .getValue()
                    .properties()
            ));
    }

    static LoadablePropertyMappings prepareProperties(
        GraphProjectFromStoreConfig graphProjectConfig,
        GraphDimensions graphDimensions,
        TransactionContext transaction
    ) {
        return prepareLoadableProperties(graphDimensions, transaction, propertyMappings(graphProjectConfig));
    }

    private static LoadablePropertyMappings prepareLoadableProperties(
        GraphDimensions dimensions,
        TransactionContext transaction,
        Map<NodeLabel, PropertyMappings> storeLoadedProperties
    ) {
        if (dimensions.tokenNodeLabelMapping() == null || !GdsFeatureToggles.USE_PROPERTY_VALUE_INDEX.isEnabled()) {
            return ImmutableLoadablePropertyMappings
                .builder()
                .putAllStoredProperties(storeLoadedProperties)
                .build();
        }

        return transaction.apply((tx, ktx) -> prepareLoadableProperties(
            dimensions,
            tx.schema(),
            ktx.schemaRead(),
            storeLoadedProperties
        ));
    }

    private static LoadablePropertyMappings prepareLoadableProperties(
        GraphDimensions dimensions,
        Schema schema,
        SchemaReadCore schemaRead,
        Map<NodeLabel, PropertyMappings> propertyMappingsByNodeLabel
    ) {
        var nodeLabelMapping = Objects.requireNonNull(dimensions.tokenNodeLabelMapping());

        var labelIds = StreamSupport
            .stream(nodeLabelMapping.keys().spliterator(), false)
            .mapToInt(labelCursor -> labelCursor.value)
            .filter(IndexPropertyMappings::labelIsCandidateForIndexScan);

        var scannableIndexes = labelIds
            .mapToObj(label -> possibleIndexesForLabel(schema, schemaRead, label))
            .flatMap(Function.identity());

        var indexPerLabel = scannableIndexes
            .flatMap(index -> conflateIndexToLabels(nodeLabelMapping, index))
            .collect(groupingBy(
                Map.Entry::getKey,
                toMap(id -> id.getValue().schema().getPropertyId(), Map.Entry::getValue)
            ));

        return groupIndexes(dimensions, propertyMappingsByNodeLabel, indexPerLabel);
    }

    private static boolean labelIsCandidateForIndexScan(int labelId) {
        return labelId != ANY_LABEL;
    }

    private static Stream<IndexDescriptor> possibleIndexesForLabel(
        Schema schema,
        SchemaReadCore schemaRead,
        int labelId
    ) {
        return Iterators
            .stream(schemaRead.indexesGetForLabel(labelId))
            .filter(index -> indexIsCandidateForIndexScan(schema, index));
    }

    private static boolean indexIsCandidateForIndexScan(Schema schema, IndexDescriptor index) {

        // no index available
        if (index == IndexDescriptor.NO_INDEX) {
            return false;
        }

        // composite index, not usable for scanning
        if (index.schema().getPropertyIds().length != 1) {
            return false;
        }

        // not a numeric index
        if (Neo4jProxy.isNotNumericIndex(index.getCapability())) {
            return false;
        }

        // check if the index is online
        try {
            // give it a second
            schema.awaitIndexOnline(index.getName(), 1, TimeUnit.SECONDS);
            // index is ok
            return true;
        } catch (RuntimeException notOnline) {
            // index not available, load via store scanning instead
            return false;
        }
    }

    private static Stream<Map.Entry<NodeLabel, IndexDescriptor>> conflateIndexToLabels(
        IntObjectMap<List<NodeLabel>> labelMapping,
        IndexDescriptor index
    ) {
        var nodeLabels = labelMapping.getOrDefault(index.schema().getLabelId(), List.of());
        return nodeLabels.stream().map(label -> Map.entry(label, index));
    }

    private static LoadablePropertyMappings groupIndexes(
        GraphDimensions dimensions,
        Map<NodeLabel, PropertyMappings> propertyMappingsByNodeLabel,
        Map<NodeLabel, Map<Integer, IndexDescriptor>> availablePropertyIndexes
    ) {
        var builder = ImmutableLoadablePropertyMappings.builder();
        availablePropertyIndexes.forEach((nodeLabel, propertyKeyToIndex) -> {
            // get possible property mapping, removing it from the set of properties to load from store
            var propertyMappings = propertyMappingsByNodeLabel.remove(nodeLabel);

            if (propertyMappings == null) {
                // property should not be loaded
                return;
            }

            // split into properties that can be loaded from the index vs store
            var storeMappingsBuilder = PropertyMappings.builder();
            var indexMappingsBuilder = ImmutableIndexedPropertyMappings.builder();

            propertyMappings.forEach(mapping -> {
                var propertyKey = dimensions.nodePropertyTokens().get(mapping.neoPropertyKey());
                if (propertyKey == null) {
                    // property should not be loaded
                    return;
                }

                var indexDescriptor = propertyKeyToIndex.get(propertyKey);
                if (indexDescriptor != null) {
                    // we got an index
                    var propertyMapping = ImmutableIndexedPropertyMapping.of(
                        mapping,
                        indexDescriptor
                    );
                    indexMappingsBuilder.addMapping(propertyMapping);
                } else {
                    // load via store scan
                    storeMappingsBuilder.addMapping(mapping);
                }
            });

            // check if we got store properties remaining that we need to load
            var storeMappings = storeMappingsBuilder.build();
            if (storeMappings.hasMappings()) {
                builder.putStoredProperty(nodeLabel, storeMappings);
            }

            // check if we got indexed properties that we need to load
            var indexMappings = indexMappingsBuilder.build();
            if (!indexMappings.mappings().isEmpty()) {
                builder.putIndexedProperty(nodeLabel, indexMappings);
            }
        });

        // add all remaining store loaded properties
        builder.putAllStoredProperties(propertyMappingsByNodeLabel);

        return builder.build();
    }

    private IndexPropertyMappings() {}

    @ValueClass
    interface IndexedPropertyMapping {

        PropertyMapping property();

        IndexDescriptor index();
    }

    @ValueClass
    interface IndexedPropertyMappings {

        List<IndexedPropertyMapping> mappings();
    }

    @ValueClass
    public interface LoadablePropertyMappings {

        Map<NodeLabel, PropertyMappings> storedProperties();

        Map<NodeLabel, IndexedPropertyMappings> indexedProperties();
    }
}


