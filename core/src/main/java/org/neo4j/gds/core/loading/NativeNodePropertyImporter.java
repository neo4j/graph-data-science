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

import com.carrotsearch.hppc.IntObjectHashMap;
import com.carrotsearch.hppc.IntObjectMap;
import com.carrotsearch.hppc.LongObjectHashMap;
import com.carrotsearch.hppc.LongObjectMap;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.PropertyMapping;
import org.neo4j.gds.PropertyMappings;
import org.neo4j.gds.api.NodeMapping;
import org.neo4j.gds.api.NodeProperties;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.compat.PropertyReference;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.loading.nodeproperties.NodePropertiesFromStoreBuilder;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.values.storable.Value;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static org.neo4j.gds.core.GraphDimensions.ANY_LABEL;
import static org.neo4j.gds.core.GraphDimensions.IGNORE;

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

    int importProperties(
        long nodeId,
        long neoNodeId,
        long[] labelIds,
        PropertyReference propertiesReference,
        KernelTransaction kernelTransaction
    ) {
        try (PropertyCursor pc = Neo4jProxy.allocatePropertyCursor(kernelTransaction)) {
            Neo4jProxy.nodeProperties(kernelTransaction, neoNodeId, propertiesReference, pc);
            int nodePropertiesRead = 0;
            while (pc.next()) {
                nodePropertiesRead += importProperty(nodeId, neoNodeId, labelIds, pc);
            }
            return nodePropertiesRead;
        }
    }

    public Map<NodeLabel, Map<PropertyMapping, NodeProperties>> result(NodeMapping nodeMapping) {
        return buildersByLabel.build(nodeMapping);
    }

    private int importProperty(long nodeId, long neoNodeId, long[] labelIds, PropertyCursor propertyCursor) {
        int propertiesImported = 0;
        int propertyKey = propertyCursor.propertyKey();

        for (long labelId : labelIds) {
            if (labelId == IGNORE || labelId == ANY_LABEL) {
                continue;
            }

            var buildersByPropertyId = buildersByLabelIdAndPropertyId.get(labelId);
            if (buildersByPropertyId != null) {
                propertiesImported += setPropertyValue(
                    nodeId,
                    neoNodeId,
                    propertyCursor,
                    propertyKey,
                    buildersByPropertyId
                );
            }
        }

        if (containsAnyLabelProjection) {
            propertiesImported += setPropertyValue(
                nodeId,
                neoNodeId,
                propertyCursor,
                propertyKey,
                buildersByLabelIdAndPropertyId.get(ANY_LABEL)
            );
        }

        return propertiesImported;
    }

    private int setPropertyValue(
        long nodeId,
        long neoNodeId, PropertyCursor propertyCursor,
        int propertyId,
        BuildersByPropertyId buildersByPropertyId
    ) {
        int propertiesImported = 0;

        List<NodePropertiesFromStoreBuilder> builders = buildersByPropertyId.get(propertyId);
        if (builders != null) {
            Value value = propertyCursor.propertyValue();

            for (NodePropertiesFromStoreBuilder builder : builders) {
                builder.set(nodeId, neoNodeId, value);
                propertiesImported++;
            }
        }

        return propertiesImported;
    }

    public static final class Builder {
        private long nodeCount;
        private Map<NodeLabel, PropertyMappings> propertyMappings;
        private GraphDimensions dimensions;
        private AllocationTracker allocationTracker = AllocationTracker.empty();


        private Builder() {
        }

        public Builder nodeCount(long nodeCount) {
            this.nodeCount = nodeCount;
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

        public Builder allocationTracker(AllocationTracker allocationTracker) {
            this.allocationTracker = allocationTracker;
            return this;
        }

        public NativeNodePropertyImporter build() {
            var nodePropertyBuilders = BuildersByLabel.create(propertyMappings, nodeCount, allocationTracker);
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
            long nodeCount,
            AllocationTracker allocationTracker
        ) {
            var instance = new BuildersByLabel();
            for (var entry : propertyMappingsByLabel.entrySet()) {
                var label = entry.getKey();
                for (var propertyMapping : entry.getValue()) {
                    var builder = NodePropertiesFromStoreBuilder.of(
                        nodeCount,
                        allocationTracker,
                        propertyMapping.defaultValue()
                    );
                    instance.put(label, propertyMapping, builder);
                }
            }
            return instance;
        }

        private final Map<NodeLabel, Map<PropertyMapping, NodePropertiesFromStoreBuilder>> builders;

        private BuildersByLabel() {
            this.builders = new HashMap<>();
        }

        private void put(NodeLabel label, PropertyMapping propertyMapping, NodePropertiesFromStoreBuilder builder) {
            builders
                .computeIfAbsent(label, __ -> new HashMap<>())
                .computeIfAbsent(propertyMapping, __ -> builder);
        }

        void forEach(BiConsumer<NodeLabel, Map<PropertyMapping, NodePropertiesFromStoreBuilder>> action) {
            builders.forEach(action);
        }

        Map<NodeLabel, Map<PropertyMapping, NodeProperties>> build(NodeMapping nodeMapping) {
            return builders
                .entrySet()
                .stream()
                .collect(Collectors.toMap(
                    Map.Entry::getKey,
                    entry -> entry.getValue().entrySet().stream().collect(Collectors.toMap(
                        Map.Entry::getKey,
                        builderEntry -> builderEntry.getValue().build(nodeMapping)
                    ))
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
