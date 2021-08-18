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
import com.carrotsearch.hppc.procedures.IntObjectProcedure;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.PropertyMapping;
import org.neo4j.gds.PropertyMappings;
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
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static org.neo4j.gds.core.GraphDimensions.ANY_LABEL;
import static org.neo4j.gds.core.GraphDimensions.IGNORE;

public final class NativeNodePropertyImporter {

    private final BuildersByNodeLabel buildersByNodeLabel;
    private final BuildersByLabelTokenAndPropertyToken buildersByLabelTokenAndPropertyToken;
    private final boolean containsAnyLabelProjection;

    public static Builder builder() {
        return new Builder();
    }

    private NativeNodePropertyImporter(
        BuildersByNodeLabel buildersByNodeLabel,
        BuildersByLabelTokenAndPropertyToken buildersByLabelTokenAndPropertyToken,
        boolean containsAnyLabelProjection
    ) {
        this.buildersByNodeLabel = buildersByNodeLabel;
        this.buildersByLabelTokenAndPropertyToken = buildersByLabelTokenAndPropertyToken;
        this.containsAnyLabelProjection = containsAnyLabelProjection;
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
                nodePropertiesRead += importProperty(nodeId, labelIds, pc);
            }
            return nodePropertiesRead;
        }
    }

    public Map<NodeLabel, Map<PropertyMapping, NodeProperties>> result() {
        return buildersByNodeLabel
            .entrySet()
            .stream()
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> entry.getValue().entrySet().stream().collect(Collectors.toMap(
                    Map.Entry::getKey,
                    builderEntry -> builderEntry.getValue().build()
                ))
            ));
    }

    private int importProperty(long nodeId, long[] labels, PropertyCursor propertyCursor) {
        int propertiesImported = 0;
        int propertyKey = propertyCursor.propertyKey();

        for (long label : labels) {
            if (label == IGNORE || label == ANY_LABEL) {
                continue;
            }

            IntObjectMap<List<NodePropertiesFromStoreBuilder>> buildersByPropertyId = buildersByLabelTokenAndPropertyToken.get((int) label);
            if (buildersByPropertyId != null) {
                propertiesImported += setPropertyValue(
                    nodeId,
                    propertyCursor,
                    propertyKey,
                    buildersByPropertyId
                );
            }
        }

        if (containsAnyLabelProjection) {
            propertiesImported += setPropertyValue(
                nodeId,
                propertyCursor,
                propertyKey,
                buildersByLabelTokenAndPropertyToken.get(ANY_LABEL)
            );
        }

        return propertiesImported;
    }

    private int setPropertyValue(
        long nodeId,
        PropertyCursor propertyCursor,
        int propertyToken,
        IntObjectMap<List<NodePropertiesFromStoreBuilder>> buildersByPropertyId
    ) {
        int propertiesImported = 0;

        List<NodePropertiesFromStoreBuilder> builders = buildersByPropertyId.get(propertyToken);
        if (builders != null) {
            Value value = propertyCursor.propertyValue();

            for (NodePropertiesFromStoreBuilder builder : builders) {
                builder.set(nodeId, value);
                propertiesImported++;
            }
        }

        return propertiesImported;
    }

    static final class BuildersByNodeLabel {
        private final Map<NodeLabel, Map<PropertyMapping, NodePropertiesFromStoreBuilder>> builders;

        BuildersByNodeLabel() {
            this.builders = new HashMap<>();
        }

        Set<Map.Entry<NodeLabel, Map<PropertyMapping, NodePropertiesFromStoreBuilder>>> entrySet() {
            return builders.entrySet();
        }

        void putIfAbsent(NodeLabel nodeLabel, HashMap<PropertyMapping, NodePropertiesFromStoreBuilder> thing) {
            builders.putIfAbsent(nodeLabel, thing);
        }

        void put(NodeLabel nodeLabel, PropertyMapping propertyMapping, NodePropertiesFromStoreBuilder builder) {
            builders.get(nodeLabel).put(propertyMapping, builder);
        }

        void forEach(BiConsumer<? super NodeLabel, ? super Map<PropertyMapping, NodePropertiesFromStoreBuilder>> action) {
            builders.forEach(action);
        }
    }

    static final class BuildersByLabelTokenAndPropertyToken {
        private final IntObjectMap<IntObjectMap<List<NodePropertiesFromStoreBuilder>>> builders;

        BuildersByLabelTokenAndPropertyToken() {
            this.builders = new IntObjectHashMap<>();
        }

        boolean containsKey(int labelId) {
            return builders.containsKey(labelId);
        }

        IntObjectMap<List<NodePropertiesFromStoreBuilder>> get(int labelId) {
            return builders.get(labelId);
        }

        void put(int labelId, int propertyToken, NodePropertiesFromStoreBuilder builder) {
            if (!builders.containsKey(labelId)) {
                builders.put(labelId, new IntObjectHashMap<>());
            }
            var buildersByPropertyToken = builders.get(labelId);
            if (!buildersByPropertyToken.containsKey(propertyToken)) {
                buildersByPropertyToken.put(propertyToken, new ArrayList<>());
            }
            buildersByPropertyToken.get(propertyToken).add(builder);
        }

    }

    public static final class Builder {
        private long nodeCount;
        private Map<NodeLabel, PropertyMappings> propertyMappingsByLabel;
        private GraphDimensions dimensions;
        private AllocationTracker tracker = AllocationTracker.empty();


        private Builder() {
        }

        public Builder nodeCount(long nodeCount) {
            this.nodeCount = nodeCount;
            return this;
        }

        public Builder propertyMappings(Map<NodeLabel, PropertyMappings> propertyMappingsByLabel) {
            this.propertyMappingsByLabel = propertyMappingsByLabel;
            return this;
        }

        public Builder dimensions(GraphDimensions dimensions) {
            this.dimensions = dimensions;
            return this;
        }

        public Builder tracker(AllocationTracker tracker) {
            this.tracker = tracker;
            return this;
        }

        public NativeNodePropertyImporter build() {
            BuildersByNodeLabel nodePropertyBuilders = initializeNodePropertyBuilders();
            BuildersByLabelTokenAndPropertyToken buildersByLabelIdAndPropertyId = buildersByLabelIdAndPropertyId(nodePropertyBuilders);
            return new NativeNodePropertyImporter(
                nodePropertyBuilders,
                buildersByLabelIdAndPropertyId,
                buildersByLabelIdAndPropertyId.containsKey(ANY_LABEL)
            );
        }

        private BuildersByNodeLabel initializeNodePropertyBuilders() {
            var builders = new BuildersByNodeLabel();
            propertyMappingsByLabel.forEach((nodeLabel, propertyMappings) -> {
                if (propertyMappings.numberOfMappings() > 0) {
                    builders.putIfAbsent(nodeLabel, new HashMap<>());
                    for (PropertyMapping propertyMapping : propertyMappings) {
                        NodePropertiesFromStoreBuilder builder = NodePropertiesFromStoreBuilder.of(
                            nodeCount, tracker, propertyMapping.defaultValue()
                        );
                        builders.put(nodeLabel, propertyMapping, builder);
                    }
                }
            });
            return builders;
        }

        private BuildersByLabelTokenAndPropertyToken buildersByLabelIdAndPropertyId(BuildersByNodeLabel buildersByIdentifier) {
            Map<NodeLabel, Integer> inverseIdentifierIdMapping = inverseIdentifierIdMapping();

            var buildersByLabelIdAndPropertyId = new BuildersByLabelTokenAndPropertyToken();
            buildersByIdentifier.forEach((labelIdentifier, builders) -> {
                int labelId = inverseIdentifierIdMapping.get(labelIdentifier);
                builders.forEach((propertyMapping, builder) -> {
                    int propertyToken = dimensions.nodePropertyTokens().get(propertyMapping.neoPropertyKey());
                    buildersByLabelIdAndPropertyId.put(labelId, propertyToken, builder);
                });
            });

            return buildersByLabelIdAndPropertyId;
        }

        private Map<NodeLabel, Integer> inverseIdentifierIdMapping() {
            HashMap<NodeLabel, Integer> inverseLabelIdentifierMapping = new HashMap<>();

            IntObjectProcedure<List<NodeLabel>> listIntObjectProcedure = (labelId, elementIdentifiers) -> {
                elementIdentifiers.forEach(identifier -> inverseLabelIdentifierMapping.put(identifier, labelId));
            };

            dimensions.tokenNodeLabelMapping().forEach(listIntObjectProcedure);

            return inverseLabelIdentifierMapping;
        }

    }


}
