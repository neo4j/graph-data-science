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
package org.neo4j.gds.projection;

import com.carrotsearch.hppc.IntObjectHashMap;
import com.carrotsearch.hppc.IntObjectMap;
import com.carrotsearch.hppc.LongObjectHashMap;
import com.carrotsearch.hppc.LongObjectMap;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.PropertyMapping;
import org.neo4j.gds.PropertyMappings;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.config.ConcurrencyConfig;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.loading.GdsNeo4jValueConverter;
import org.neo4j.gds.core.loading.NodeLabelTokenSet;
import org.neo4j.gds.core.loading.nodeproperties.NodePropertiesFromStoreBuilder;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.storageengine.api.PropertySelection;
import org.neo4j.storageengine.api.Reference;
import org.neo4j.values.storable.DoubleArray;
import org.neo4j.values.storable.FloatArray;
import org.neo4j.values.storable.FloatingPointValue;
import org.neo4j.values.storable.IntegralValue;
import org.neo4j.values.storable.LongArray;
import org.neo4j.values.storable.Value;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import static java.util.stream.Collectors.toMap;
import static org.neo4j.gds.core.GraphDimensions.ANY_LABEL;
import static org.neo4j.gds.core.GraphDimensions.IGNORE;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class NativeNodePropertyImporter {

    private final BuildersByLabel buildersByLabel;
    private final BuildersByLabelIdAndPropertyId buildersByLabelIdAndPropertyId;
    private final boolean containsAnyLabelProjection;

    public static Builder builder() {
        return new Builder();
    }

    private NativeNodePropertyImporter(
        BuildersByLabel buildersByLabel,
        BuildersByLabelIdAndPropertyId buildersByLabelIdAndPropertyId
    ) {
        this.buildersByLabel = buildersByLabel;
        this.buildersByLabelIdAndPropertyId = buildersByLabelIdAndPropertyId;
        this.containsAnyLabelProjection = buildersByLabelIdAndPropertyId.containsAnyLabelProjection();
        // TODO: create a union of all property keys for all labels and use that one to filter the property cursor on 4.4-dev
    }

    public int importProperties(
        long neoNodeId,
        NodeLabelTokenSet labelTokens,
        Reference propertiesReference,
        KernelTransaction kernelTransaction
    ) {
        try (
            PropertyCursor pc = kernelTransaction
                .cursors()
                .allocatePropertyCursor(kernelTransaction.cursorContext(), kernelTransaction.memoryTracker())
        ) {
            kernelTransaction
                .dataRead()
                .nodeProperties(neoNodeId, propertiesReference, PropertySelection.ALL_PROPERTIES, pc);
            int nodePropertiesRead = 0;
            while (pc.next()) {
                nodePropertiesRead += importProperty(neoNodeId, labelTokens, pc);
            }
            return nodePropertiesRead;
        }
    }

    public Map<PropertyMapping, NodePropertyValues> result(IdMap idMap) {
        return buildersByLabel.build(idMap);
    }

    private int importProperty(long neoNodeId, NodeLabelTokenSet labelTokens, PropertyCursor propertyCursor) {
        int propertiesImported = 0;
        int propertyKey = propertyCursor.propertyKey();

        for (int i = 0; i < labelTokens.length(); i++) {
            var labelId = labelTokens.get(i);
            if (labelId == IGNORE || labelId == ANY_LABEL) {
                continue;
            }

            var buildersByPropertyId = buildersByLabelIdAndPropertyId.get(labelId);
            if (buildersByPropertyId != null) {
                propertiesImported += setPropertyValue(
                    neoNodeId,
                    propertyCursor,
                    propertyKey,
                    buildersByPropertyId
                );
            }
        }

        if (containsAnyLabelProjection) {
            propertiesImported += setPropertyValue(
                neoNodeId,
                propertyCursor,
                propertyKey,
                buildersByLabelIdAndPropertyId.get(ANY_LABEL)
            );
        }

        return propertiesImported;
    }

    private int setPropertyValue(
        long neoNodeId,
        PropertyCursor propertyCursor,
        int propertyId,
        BuildersByPropertyId buildersByPropertyId
    ) {
        int propertiesImported = 0;

        List<NodePropertiesFromStoreBuilder> builders = buildersByPropertyId.get(propertyId);
        if (builders != null) {
            Value value = propertyCursor.propertyValue();

            for (NodePropertiesFromStoreBuilder builder : builders) {
                verifyValueType(value);
                var gdsValue = GdsNeo4jValueConverter.toValue(value);
                builder.set(neoNodeId, gdsValue);
                propertiesImported++;
            }
        }

        return propertiesImported;
    }

    private void verifyValueType(Value value) {
        if (!(
            value instanceof IntegralValue ||
                value instanceof FloatingPointValue ||
                value instanceof LongArray ||
                value instanceof DoubleArray ||
                value instanceof FloatArray
        )) {
            throw new UnsupportedOperationException(formatWithLocale(
                "Loading of values of type %s is currently not supported",
                value.getTypeName()
            ));
        }
    }

    public static final class Builder {
        private Concurrency concurrency = ConcurrencyConfig.TYPED_DEFAULT_CONCURRENCY;
        private Map<NodeLabel, PropertyMappings> propertyMappings;
        private GraphDimensions dimensions;


        private Builder() {
        }

        public Builder concurrency(Concurrency concurrency) {
            this.concurrency = concurrency;
            return this;
        }

        public Builder propertyMappings(Map<NodeLabel, PropertyMappings> propertyMappingsByLabel) {
            this.propertyMappings = propertyMappingsByLabel;
            return this;
        }

        public Builder dimensions(GraphDimensions dimensions) {
            this.dimensions = dimensions;
            return this;
        }

        public NativeNodePropertyImporter build() {
            var nodePropertyBuilders = BuildersByLabel.create(
                propertyMappings,
                concurrency
            );
            var buildersByLabelIdAndPropertyId = BuildersByLabelIdAndPropertyId.create(
                nodePropertyBuilders,
                // TODO: We align on `id` over `token` in this class but need to carry that change on to the rest of
                //       the loading logic
                dimensions.tokenNodeLabelMapping(),
                dimensions.nodePropertyTokens()
            );
            return new NativeNodePropertyImporter(nodePropertyBuilders, buildersByLabelIdAndPropertyId);
        }
    }

    static final class BuildersByLabel {

        static BuildersByLabel create(
            Map<NodeLabel, PropertyMappings> propertyMappingsByLabel,
            Concurrency concurrency
        ) {
            var propertyBuildersByKey = new HashMap<String, NodePropertiesFromStoreBuilder>();

             propertyMappingsByLabel
                 .values()
                 .stream()
                 .flatMap(propertyMappings -> propertyMappings.mappings().stream())
                 .forEach(propertyMapping -> propertyBuildersByKey.putIfAbsent(
                     propertyMapping.propertyKey(),
                     NodePropertiesFromStoreBuilder.of(
                         propertyMapping.defaultValue(),
                         concurrency
                     )
                 ));

            var instance = new BuildersByLabel();
            for (var entry : propertyMappingsByLabel.entrySet()) {
                var label = entry.getKey();
                for (var propertyMapping : entry.getValue()) {
                    instance.put(label, propertyMapping, propertyBuildersByKey.get(propertyMapping.propertyKey()));
                }
            }
            return instance;
        }

        private final Map<String, NodePropertiesFromStoreBuilder> buildersByPropertyKey;
        private final Map<String, PropertyMapping> propertyMappings;
        private final Map<NodeLabel, Map<PropertyMapping, NodePropertiesFromStoreBuilder>> buildersByLabel;

        private BuildersByLabel() {
            this.buildersByLabel = new HashMap<>();
            this.propertyMappings = new HashMap<>();
            this.buildersByPropertyKey = new HashMap<>();
        }

        private void put(NodeLabel label, PropertyMapping propertyMapping, NodePropertiesFromStoreBuilder builder) {
            propertyMappings.put(propertyMapping.propertyKey(), propertyMapping);
            buildersByPropertyKey.put(propertyMapping.propertyKey(), builder);
            buildersByLabel
                .computeIfAbsent(label, __ -> new HashMap<>())
                .computeIfAbsent(propertyMapping, __ -> builder);
        }

        void forEach(BiConsumer<NodeLabel, Map<PropertyMapping, NodePropertiesFromStoreBuilder>> action) {
            buildersByLabel.forEach(action);
        }

        Map<PropertyMapping, NodePropertyValues> build(IdMap idMap) {
            return buildersByPropertyKey
                .entrySet()
                .stream()
                .collect(toMap(
                    entry -> propertyMappings.get(entry.getKey()),
                    entry -> entry.getValue().build(idMap)
                ));
        }
    }

    static final class BuildersByLabelIdAndPropertyId {

        static BuildersByLabelIdAndPropertyId create(
            BuildersByLabel buildersByLabel,
            IntObjectMap<List<NodeLabel>> labelsByLabelId,
            Map<String, Integer> propertyIds
        ) {
            var labelIdByLabel = LabelIdByLabel.create(labelsByLabelId);

            var instance = new BuildersByLabelIdAndPropertyId();
            buildersByLabel.forEach((labelIdentifier, builders) -> {
                int labelId = labelIdByLabel.get(labelIdentifier);
                builders.forEach((propertyMapping, builder) -> {
                    int propertyId = propertyIds.get(propertyMapping.neoPropertyKey());
                    instance.put(labelId, propertyId, builder);
                });
            });

            return instance;
        }

        private final LongObjectMap<BuildersByPropertyId> builders;

        boolean containsAnyLabelProjection() {
            return builders.containsKey(ANY_LABEL);
        }

        BuildersByPropertyId get(long labelId) {
            return builders.get(labelId);
        }

        void put(int labelId, int propertyId, NodePropertiesFromStoreBuilder builder) {
            if (!builders.containsKey(labelId)) {
                builders.put(labelId, new BuildersByPropertyId());
            }
            builders.get(labelId).add(propertyId, builder);
        }

        private BuildersByLabelIdAndPropertyId() {
            this.builders = new LongObjectHashMap<>();
        }
    }

    static final class BuildersByPropertyId {
        private final IntObjectMap<List<NodePropertiesFromStoreBuilder>> builders;

        List<NodePropertiesFromStoreBuilder> get(int propertyId) {
            return builders.get(propertyId);
        }

        void add(int propertyId, NodePropertiesFromStoreBuilder builder) {
            if (!builders.containsKey(propertyId)) {
                builders.put(propertyId, new ArrayList<>());
            }
            builders.get(propertyId).add(builder);
        }

        private BuildersByPropertyId() {
            this.builders = new IntObjectHashMap<>();
        }
    }

    static final class LabelIdByLabel {

        static LabelIdByLabel create(IntObjectMap<List<NodeLabel>> mapping) {
            var instance = new LabelIdByLabel();
            for (IntObjectCursor<List<NodeLabel>> cursor : mapping) {
                for (NodeLabel label : cursor.value) {
                    instance.labelIdByLabel.put(label, cursor.key);
                }
            }
            return instance;
        }

        private final Map<NodeLabel, Integer> labelIdByLabel;

        int get(NodeLabel label) {
            return labelIdByLabel.get(label);
        }

        private LabelIdByLabel() {
            labelIdByLabel = new HashMap<>();
        }
    }
}
