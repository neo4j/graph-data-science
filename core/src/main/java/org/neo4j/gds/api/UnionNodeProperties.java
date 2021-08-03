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
package org.neo4j.gds.api;

import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import java.util.Map;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.stream.Collectors;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class UnionNodeProperties implements NodeProperties {

    private final ValueType valueType;
    private final NodeMapping nodeMapping;
    private final Map<NodeLabel, NodeProperties> labelToNodePropertiesMap;
    private final ValueProducer valueProducer;
    private final NodeLabel[] availableNodeLabels;

    public UnionNodeProperties(NodeMapping nodeMapping, Map<NodeLabel, NodeProperties> labelToNodePropertiesMap) {
        this.nodeMapping = nodeMapping;
        this.labelToNodePropertiesMap = labelToNodePropertiesMap;
        this.availableNodeLabels = nodeMapping.availableNodeLabels().toArray(NodeLabel[]::new);

        var valueTypes = labelToNodePropertiesMap.values()
            .stream()
            .map(NodeProperties::valueType)
            .collect(Collectors.toList());

        var expectedType = valueTypes.get(0);
        if (valueTypes.stream().anyMatch(actualType -> actualType != expectedType)) {
            throw new IllegalArgumentException(formatWithLocale(
                "UnionProperties must all have the same type, but found %s",
                valueTypes
            ));
        }

        this.valueType = expectedType;

        switch(valueType) {
            case LONG:
                this.valueProducer = ((unp, nodeId) -> Values.longValue(unp.longValue(nodeId)));
                break;
            case DOUBLE:
                this.valueProducer = ((unp, nodeId) -> Values.doubleValue(unp.doubleValue(nodeId)));
                break;
            case LONG_ARRAY:
                this.valueProducer = ((unp, nodeId) -> Values.longArray(unp.longArrayValue(nodeId)));
                break;
            case FLOAT_ARRAY:
                this.valueProducer = ((unp, nodeId) -> Values.floatArray(unp.floatArrayValue(nodeId)));
                break;
            case DOUBLE_ARRAY:
                this.valueProducer = ((unp, nodeId) -> Values.doubleArray(unp.doubleArrayValue(nodeId)));
                break;
            default:
                throw new UnsupportedOperationException(formatWithLocale("No value converter for ValueType %s", valueTypes));
        }
    }

    @Override
    public double doubleValue(long nodeId) {
        if (valueType == ValueType.DOUBLE || valueType == ValueType.LONG) {
            var nodeProperties = getPropertiesForNodeId(nodeId);
            return nodeProperties == null ? DefaultValue.DOUBLE_DEFAULT_FALLBACK : nodeProperties.doubleValue(nodeId);
        } else {
            throw new UnsupportedOperationException(formatWithLocale(
                "Cannot cast properties of type %s to double",
                valueType
            ));
        }
    }

    @Override
    public long longValue(long nodeId) {
        if (valueType == ValueType.LONG) {
            var nodeProperties = getPropertiesForNodeId(nodeId);
            return nodeProperties == null ? DefaultValue.LONG_DEFAULT_FALLBACK : nodeProperties.longValue(nodeId);
        } else {
            throw new UnsupportedOperationException(formatWithLocale(
                "Cannot cast properties of type %s to long",
                valueType
            ));
        }
    }

    @Override
    public double[] doubleArrayValue(long nodeId) {
        if ((valueType == ValueType.DOUBLE_ARRAY) || (valueType == ValueType.FLOAT_ARRAY)) {
            var nodeProperties = getPropertiesForNodeId(nodeId);
            return nodeProperties == null ? null : nodeProperties.doubleArrayValue(nodeId);
        } else {
            throw new UnsupportedOperationException(formatWithLocale(
                "Cannot cast properties of type %s to double array",
                valueType
            ));
        }
    }

    @Override
    public float[] floatArrayValue(long nodeId) {
        if (valueType == ValueType.FLOAT_ARRAY) {
            var nodeProperties = getPropertiesForNodeId(nodeId);
            return nodeProperties == null ? null : nodeProperties.floatArrayValue(nodeId);
        } else {
            throw new UnsupportedOperationException(formatWithLocale(
                "Cannot cast properties of type %s to float array",
                valueType
            ));
        }
    }

    @Override
    public long[] longArrayValue(long nodeId) {
        if (valueType == ValueType.LONG_ARRAY) {
            var nodeProperties = getPropertiesForNodeId(nodeId);
            return nodeProperties == null ? null : nodeProperties.longArrayValue(nodeId);
        } else {
            throw new UnsupportedOperationException(formatWithLocale(
                "Cannot cast properties of type %s to long array",
                valueType
            ));
        }
    }

    @Override
    public Object getObject(long nodeId) {
        var nodeProperties = getPropertiesForNodeId(nodeId);
        return nodeProperties == null ? null : nodeProperties.getObject(nodeId);
    }

    @Override
    public Value value(long nodeId) {
        return valueProducer.getValue(this, nodeId);
    }

    @Override
    public ValueType valueType() {
        return valueType;
    }

    @Override
    public long release() {
        return 0;
    }

    private NodeProperties getPropertiesForNodeId(long nodeId) {
        for (NodeLabel label : availableNodeLabels) {
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

    @FunctionalInterface
    private interface ValueProducer {
        Value getValue(UnionNodeProperties unionNodeProperties, long nodeId);
    }

    @Override
    public OptionalLong getMaxLongPropertyValue() {
        return OptionalLong.empty();
    }

    @Override
    public OptionalDouble getMaxDoublePropertyValue() {
        return OptionalDouble.empty();
    }
}
