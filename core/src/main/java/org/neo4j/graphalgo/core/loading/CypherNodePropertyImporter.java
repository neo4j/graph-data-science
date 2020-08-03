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
import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.api.DefaultValue;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.api.nodeproperties.ValueType;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.neo4j.graphalgo.NodeLabel.ALL_NODES;
import static org.neo4j.graphalgo.core.GraphDimensions.ANY_LABEL;
import static org.neo4j.graphalgo.core.GraphDimensions.IGNORE;

public class CypherNodePropertyImporter {

    public static final DefaultValue NO_PROPERTY_VALUE = DefaultValue.DEFAULT;

    private final Collection<String> propertyColumns;
    private final long nodeCount;
    private final IntObjectMap<List<NodeLabel>> labelTokenNodeLabelMapping;
    private final Map<NodeLabel, Map<String, NodePropertiesBuilder>> buildersByNodeLabel;


    public CypherNodePropertyImporter(
        Collection<String> propertyColumns,
        IntObjectMap<List<NodeLabel>> labelTokenNodeLabelMapping,
        long nodeCount
    ) {
        this.propertyColumns = propertyColumns;
        this.labelTokenNodeLabelMapping = labelTokenNodeLabelMapping;
        this.nodeCount = nodeCount;

        this.buildersByNodeLabel = new HashMap<>();
    }

    public Collection<String> propertyColumns() {
        return propertyColumns;
    }

    public void registerPropertiesForLabels(List<String> labels) {
        for (String label : labels) {
            NodeLabel nodeLabel = new NodeLabel(label);
            Map<String, NodePropertiesBuilder> propertyBuilders = buildersByNodeLabel.computeIfAbsent(
                nodeLabel,
                (ignore) -> new HashMap<>()
            );
            for (String property : propertyColumns) {
                propertyBuilders.computeIfAbsent(
                    property,
                    (ignore) -> NodePropertiesBuilder.of(
                        nodeCount, ValueType.DOUBLE, AllocationTracker.EMPTY, NO_PROPERTY_VALUE
                    )
                );
            }
        }
    }

    public int importProperties(long nodeId, long[] labels, Map<String, Number> nodeProperties) {
        int propertiesImported = 0;

        // If there is a node projection for ANY label, then we need to consume the node properties regardless.
        propertiesImported += setPropertyForLabel(ALL_NODES, nodeProperties, nodeId);

        for (long label : labels) {
            if (label == IGNORE || label == ANY_LABEL) {
                continue;
            }

            for (NodeLabel labelIdentifier : labelTokenNodeLabelMapping.get((int) label)) {
                propertiesImported += setPropertyForLabel(labelIdentifier, nodeProperties, nodeId);
            }
        }

        return propertiesImported;
    }

    public Map<NodeLabel, Map<PropertyMapping, NodeProperties>> result() {
        return buildersByNodeLabel
            .entrySet()
            .stream()
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> entry.getValue().entrySet().stream().collect(Collectors.toMap(
                    builderEntry -> PropertyMapping.of(builderEntry.getKey(), DefaultValue.of(Double.NaN)),
                    builderEntry -> builderEntry.getValue().build()
                ))
            ));
    }

    private int setPropertyForLabel(
        NodeLabel labelIdentifier,
        Map<String, Number> nodeProperties,
        long nodeId
    ) {
        int propertiesImported = 0;

        if (buildersByNodeLabel.containsKey(labelIdentifier)) {
            Map<String, NodePropertiesBuilder> buildersByProperty = buildersByNodeLabel.get(labelIdentifier);

            for (Map.Entry<String, Number> propertyEntry : nodeProperties.entrySet()) {
                if (buildersByProperty.containsKey(propertyEntry.getKey())) {
                    NodePropertiesBuilder builder = buildersByProperty.get(propertyEntry.getKey());
                    builder.set(nodeId, propertyEntry.getValue().doubleValue());
                    propertiesImported++;
                }
            }
        }

        return propertiesImported;
    }

}
