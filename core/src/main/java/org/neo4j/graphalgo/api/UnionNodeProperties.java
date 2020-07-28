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
package org.neo4j.graphalgo.api;

import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.api.nodeproperties.ValueType;

import java.util.Map;
import java.util.stream.Collectors;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

public class UnionNodeProperties implements NodeProperties {

    private final ValueType valueType;
    private final NodeMapping nodeMapping;
    private final Map<NodeLabel, NodeProperties> labelToNodePropertiesMap;


    public UnionNodeProperties(NodeMapping nodeMapping, Map<NodeLabel, NodeProperties> labelToNodePropertiesMap) {
        this.nodeMapping = nodeMapping;
        this.labelToNodePropertiesMap = labelToNodePropertiesMap;

        var valueTypes = labelToNodePropertiesMap.values()
            .stream()
            .map(NodeProperties::getType)
            .collect(Collectors.toList());

        var expectedType = valueTypes.get(0);
        if (valueTypes.stream().anyMatch(actualType -> actualType != expectedType)) {
            throw new IllegalArgumentException(formatWithLocale(
                "UnionProperties must all have the same type, but found %s",
                valueTypes
            ));
        }

        this.valueType = expectedType;
    }

    @Override
    public double getDouble(long nodeId) {
        return getDouble(nodeId, Double.NaN);
    }

    @Override
    public double getDouble(long nodeId, double defaultValue) {
        if (valueType == ValueType.DOUBLE || valueType == ValueType.LONG) {
            var nodeProperties = getPropertiesForNodeId(nodeId);
            return nodeProperties == null ? defaultValue : nodeProperties.getDouble(nodeId);
        } else {
            throw new UnsupportedOperationException(formatWithLocale(
                "Cannot cast properties of type %s to double",
                valueType
            ));
        }
    }

    @Override
    public long getLong(long nodeId) {
        return getLong(nodeId, Long.MIN_VALUE);
    }

    @Override
    public long getLong(long nodeId, long defaultValue) {
        // TODO forbid doubles once we load properties with their correct type
        if (valueType == ValueType.LONG || valueType == ValueType.DOUBLE) {
            var nodeProperties = getPropertiesForNodeId(nodeId);
            return nodeProperties == null ? defaultValue : nodeProperties.getLong(nodeId);
        } else {
            throw new UnsupportedOperationException(formatWithLocale(
                "Cannot cast properties of type %s to long",
                valueType
            ));
        }
    }

    @Override
    public double[] getDoubleArray(long nodeId) {
        return getDoubleArray(nodeId, null);
    }

    @Override
    public double[] getDoubleArray(long nodeId, double[] defaultValue) {
        if (valueType == ValueType.DOUBLE_ARRAY) {
            var nodeProperties = getPropertiesForNodeId(nodeId);
            return nodeProperties == null ? defaultValue : nodeProperties.getDoubleArray(nodeId);
        } else {
            throw new UnsupportedOperationException(formatWithLocale(
                "Cannot cast properties of type %s to double array",
                valueType
            ));
        }
    }

    @Override
    public Object getObject(long nodeId) {
        return getObject(nodeId, null);
    }

    @Override
    public Object getObject(long nodeId, Object defaultValue) {
        var nodeProperties = getPropertiesForNodeId(nodeId);
        return nodeProperties == null ? defaultValue : nodeProperties.getObject(nodeId);
    }

    @Override
    public ValueType getType() {
        return valueType;
    }

    @Override
    public long release() {
        return 0;
    }

    private NodeProperties getPropertiesForNodeId(long nodeId) {
        for (NodeLabel label : nodeMapping.availableNodeLabels()) {
            if (nodeMapping.hasLabel(nodeId, label)) {
                NodeProperties nodeProperties = labelToNodePropertiesMap.get(label);
                if (nodeProperties != null) {
                    // This returns the property value for the first label that has the property.
                    // If there are multiple labels with the same property key, but different values,
                    // this might lead to issues.
                    // TODO: find out if this is an actual problem
                    return nodeProperties;
                }
            }
        }

        return null;
    }

    @Override
    public long size() {
        return labelToNodePropertiesMap.values().stream()
            .map(NodeProperties::size)
            .reduce(Long::sum)
            .orElse(0L);
    }
}
