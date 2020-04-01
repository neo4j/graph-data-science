package org.neo4j.graphalgo.core.loading;

import com.carrotsearch.hppc.procedures.LongObjectProcedure;
import org.neo4j.graphalgo.ElementIdentifier;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.PropertyMappings;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.values.storable.NumberValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.neo4j.graphalgo.core.loading.NodesBatchBuffer.ANY_LABEL;
import static org.neo4j.graphalgo.core.loading.NodesBatchBuffer.IGNORE_LABEL;

public class NativeNodePropertyImporter {

    private final Map<ElementIdentifier, Map<PropertyMapping, NodePropertiesBuilder>> buildersByIdentifier;
    private final Map<Long, Map<Integer, List<NodePropertiesBuilder>>> buildersByLabelIdAndPropertyId;
    private final Map<Integer, List<NodePropertiesBuilder>> anyLabelImporters;

    public static Builder builder() {
        return new Builder();
    }

    public NativeNodePropertyImporter(
            Map<ElementIdentifier, Map<PropertyMapping, NodePropertiesBuilder>> buildersByIdentifier,
            Map<Long, Map<Integer, List<NodePropertiesBuilder>>> buildersByLabelIdAndPropertyId
    ) {
        this.buildersByIdentifier = buildersByIdentifier;
        this.buildersByLabelIdAndPropertyId = buildersByLabelIdAndPropertyId;
        this.anyLabelImporters = buildersByLabelIdAndPropertyId.get((long) ANY_LABEL);
    }

    public int importProperties(
        long nodeId,
        long neoNodeId,
        long[] labelIds,
        long propertiesReference,
        CursorFactory cursors,
        Read read
    ) {
        try (PropertyCursor pc = cursors.allocatePropertyCursor()) {
            read.nodeProperties(neoNodeId, propertiesReference, pc);
            int nodePropertiesRead = 0;
            while (pc.next()) {
                nodePropertiesRead += importProperty(nodeId, labelIds, pc);
            }
            return nodePropertiesRead;
        }
    }

    public Map<ElementIdentifier, Map<PropertyMapping, NodeProperties>> result() {
        return buildersByIdentifier
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
            if (label == IGNORE_LABEL || label == ANY_LABEL) {
                continue;
            }

            if (buildersByLabelIdAndPropertyId.containsKey(label)) {
                Map<Integer, List<NodePropertiesBuilder>> buildersByPropertyId = buildersByLabelIdAndPropertyId.get(label);
                setPropertyValue(nodeId, propertyCursor, propertyKey, buildersByPropertyId);
            }
        }

        if (anyLabelImporters != null) {
            setPropertyValue(nodeId, propertyCursor, propertyKey, anyLabelImporters);
        }

        return propertiesImported;
    }

    private void setPropertyValue(long nodeId, PropertyCursor propertyCursor, int propertyKey, Map<Integer, List<NodePropertiesBuilder>> buildersByPropertyId) {
        if (buildersByPropertyId.containsKey(propertyKey)) {
            List<NodePropertiesBuilder> builders = buildersByPropertyId.get(propertyKey);
            Value value = propertyCursor.propertyValue();

            if (value instanceof NumberValue) {
                for (NodePropertiesBuilder builder : builders) {
                    builder.set(nodeId, ((NumberValue) value).doubleValue());
                }
            } else if (!Values.NO_VALUE.equals(value)) {
                throw new IllegalArgumentException(String.format(
                        "Unsupported type [%s] of value %s. Please use a numeric property.",
                        value.valueGroup(),
                        value));
            }
        }
    }

    public static class Builder {
        private long nodeCount;
        private Map<ElementIdentifier, PropertyMappings> propertyMappings;
        private GraphDimensions dimensions;
        private int concurrency;
        private AllocationTracker tracker;


        private Builder() {
        }

        public Builder nodeCount(long nodeCount) {
            this.nodeCount = nodeCount;
            return this;
        }

        public Builder propertyMappings(Map<ElementIdentifier, PropertyMappings> propertyMappings) {
            this.propertyMappings = propertyMappings;
            return this;
        }

        public Builder dimensions(GraphDimensions dimensions) {
            this.dimensions = dimensions;
            return this;
        }

        public Builder concurrency(int concurrency) {
            this.concurrency = concurrency;
            return this;
        }

        public Builder tracker(AllocationTracker tracker) {
            this.tracker = tracker;
            return this;
        }

        public NativeNodePropertyImporter build() {
            Map<ElementIdentifier, Map<PropertyMapping, NodePropertiesBuilder>> nodePropertyBuilders = initializeNodePropertyBuilders();
            Map<Long, Map<Integer, List<NodePropertiesBuilder>>> buildersByLabelIdAndPropertyId = buildersByLabelIdAndPropertyId(nodePropertyBuilders);
            return new NativeNodePropertyImporter(
                    nodePropertyBuilders,
                    buildersByLabelIdAndPropertyId
            );
        }

        private Map<ElementIdentifier, Map<PropertyMapping, NodePropertiesBuilder>> initializeNodePropertyBuilders() {
            Map<ElementIdentifier, Map<PropertyMapping, NodePropertiesBuilder>> builders = new HashMap<>();
            propertyMappings.forEach((nodeLabel, propertyMappings) -> {
                builders.putIfAbsent(nodeLabel, new HashMap<>());
                for (PropertyMapping propertyMapping : propertyMappings) {
                    NodePropertiesBuilder builder = NodePropertiesBuilder.of(
                            nodeCount,
                            tracker,
                            propertyMapping.defaultValue(),
                            dimensions.nodePropertyIds().get(propertyMapping.neoPropertyKey()),
                            propertyMapping.propertyKey(),
                            concurrency
                    );
                    builders.get(nodeLabel).put(propertyMapping, builder);
                }
            });
            return builders;
        }

        private Map<Long, Map<Integer, List<NodePropertiesBuilder>>> buildersByLabelIdAndPropertyId(Map<ElementIdentifier, Map<PropertyMapping, NodePropertiesBuilder>> buildersByIdentifier) {
            Map<ElementIdentifier, Long> inverseIdentifierIdMapping = inverseIdentifierIdMapping();

            Map<Long, Map<Integer, List<NodePropertiesBuilder>>> buildersByLabelIdAndPropertyId = new HashMap<>();
            buildersByIdentifier.forEach((labelIdentifier, builders) -> {
                long labelId = inverseIdentifierIdMapping.get(labelIdentifier);

                Map<Integer, List<NodePropertiesBuilder>> buildersByPropertyId =
                        buildersByLabelIdAndPropertyId.computeIfAbsent(labelId, (ignore) -> new HashMap<>());

                builders.forEach((propertyMapping, builder) -> {
                    int propertyId = dimensions.nodePropertyIds().get(propertyMapping.neoPropertyKey());

                    List<NodePropertiesBuilder> builderList = buildersByPropertyId.computeIfAbsent(propertyId, (ignore) -> new ArrayList<>());

                    builderList.add(builder);
                });
            });

            return buildersByLabelIdAndPropertyId;
        }

        private Map<ElementIdentifier, Long> inverseIdentifierIdMapping() {
            HashMap<ElementIdentifier, Long> inverseLabelIdentifierMapping = new HashMap<>();

            LongObjectProcedure<List<ElementIdentifier>> listLongObjectProcedure = (labelId, elementIdentifiers) -> {
                elementIdentifiers.forEach(identifier -> inverseLabelIdentifierMapping.put(identifier, labelId));
            };

            dimensions.labelElementIdentifierMapping().forEach(listLongObjectProcedure);

            return inverseLabelIdentifierMapping;
        }

    }


}
