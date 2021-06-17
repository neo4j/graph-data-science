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
package org.neo4j.graphalgo.core.loading.construction;

import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.api.UnionNodeProperties;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public final class NodePropertiesTestHelper {

    private NodePropertiesTestHelper() {}

    public static Map<String, NodeProperties> unionNodePropertiesOrThrow(NodesBuilder.NodeMappingAndProperties nodeMappingAndProperties) {
        return unionNodeProperties(nodeMappingAndProperties)
            .orElseThrow(() -> new IllegalArgumentException("Expected node properties to be present"));
    }

    static Optional<Map<String, NodeProperties>> unionNodeProperties(NodesBuilder.NodeMappingAndProperties nodeMappingAndProperties) {
        var nodeProperties = nodeMappingAndProperties.nodeProperties();
        var nodeMapping = nodeMappingAndProperties.nodeMapping();
        if (nodeProperties.isEmpty()) {
            return Optional.empty();
        }

        Map<String, Map<NodeLabel, NodeProperties>> nodePropertiesByKeyAndLabel = new HashMap<>();
        nodeProperties.get().forEach((nodeLabel, propertiesByKey) -> {
            propertiesByKey.forEach((propertyKey, propertyValues) -> {
                nodePropertiesByKeyAndLabel
                    .computeIfAbsent(propertyKey, __ -> new HashMap<>())
                    .put(nodeLabel, propertyValues);
            });
        });

        Map<String, NodeProperties> unionNodeProperties = nodePropertiesByKeyAndLabel
            .entrySet()
            .stream()
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> new UnionNodeProperties(nodeMapping, entry.getValue())
            ));
        return Optional.of(unionNodeProperties);
    }
}
