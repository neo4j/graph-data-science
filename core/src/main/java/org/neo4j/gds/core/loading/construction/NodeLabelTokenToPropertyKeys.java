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
package org.neo4j.gds.core.loading.construction;

import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.api.schema.NodeSchema;
import org.neo4j.gds.api.schema.PropertySchema;
import org.neo4j.gds.utils.StringJoining;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

abstract class NodeLabelTokenToPropertyKeys {

    /**
     * Creates a thread-safe, mutable mapping.
     * <p>
     * The property schemas are inferred from the input data.
     */
    static NodeLabelTokenToPropertyKeys lazy() {
        return new Lazy();
    }

    /**
     * Creates thread-safe, immutable mapping.
     * <p>
     * The property schemas are inferred from given schema.
     */
    static NodeLabelTokenToPropertyKeys fixed(NodeSchema nodeSchema) {
        return new Fixed(nodeSchema);
    }

    /**
     * Assign the given property keys to the given label token.
     * <p>
     * If the token is already present, the property keys are added with set semantics.
     */
    abstract void add(NodeLabelToken nodeLabelToken, Iterable<String> propertyKeys);

    /**
     * Returns all node labels in this mapping.
     */
    abstract Set<NodeLabel> nodeLabels();

    /**
     * Return the property schemas for the given node label.
     */
    abstract Map<String, PropertySchema> propertySchemas(
        NodeLabel nodeLabelToken,
        Map<String, PropertySchema> importPropertySchemas
    );

    /**
     * Computes the union of the two given mappings without
     * changing the contents of the mappings themselves.
     */
    static NodeLabelTokenToPropertyKeys union(
        NodeLabelTokenToPropertyKeys left,
        NodeLabelTokenToPropertyKeys right,
        Map<String, PropertySchema> importPropertySchemas
    ) {
        var union = NodeLabelTokenToPropertyKeys.lazy();

        left.nodeLabels().forEach(nodeLabel -> {
            var propertyKeys = left.propertySchemas(nodeLabel, importPropertySchemas).keySet();
            union.add(NodeLabelTokens.ofNodeLabel(nodeLabel), propertyKeys);
        });
        right.nodeLabels().forEach(nodeLabel -> {
            var propertyKeys = right.propertySchemas(nodeLabel, importPropertySchemas).keySet();
            union.add(NodeLabelTokens.ofNodeLabel(nodeLabel), propertyKeys);
        });

        return union;
    }

    private static class Fixed extends NodeLabelTokenToPropertyKeys {

        private final NodeSchema nodeSchema;

        Fixed(NodeSchema nodeSchema) {
            this.nodeSchema = nodeSchema;
        }

        @Override
        void add(NodeLabelToken nodeLabelToken, Iterable<String> propertyKeys) {
            // silence is golden
        }

        @Override
        Set<NodeLabel> nodeLabels() {
            return nodeSchema.availableLabels();
        }

        @Override
        Map<String, PropertySchema> propertySchemas(
            NodeLabel nodeLabel,
            Map<String, PropertySchema> importPropertySchemas
        ) {
            var userDefinedPropertySchemas = nodeSchema.get(nodeLabel).properties();

            // We validate that the property schemas we read during import have
            // at least a matching key and a matching type. We cannot do an
            // equality check because we cannot infer the default value or the
            // property state from just looking at the values.
            var overlap = importPropertySchemas
                .keySet()
                .stream()
                .filter(userDefinedPropertySchemas::containsKey)
                .collect(Collectors.toSet());

            if (overlap.size() < userDefinedPropertySchemas.size()) {
                var keySet = new HashSet<>(userDefinedPropertySchemas.keySet());
                keySet.removeAll(overlap);
                throw new IllegalStateException("Missing node properties during import. " +
                                                "The following keys were part of the schema, " +
                                                "but not contained in the input data: " +
                                                StringJoining.join(keySet)
                );
            }

            // We got the same keys and can check the types.
            var keysWithIncompatibleTypes = overlap.stream()
                .filter(propertyKey -> userDefinedPropertySchemas
                                           .get(propertyKey)
                                           .valueType() != importPropertySchemas
                                           .get(propertyKey)
                                           .valueType()).collect(Collectors.toSet());

            if (!keysWithIncompatibleTypes.isEmpty()) {
                throw new IllegalStateException("Incompatible value types between input schema and input data. " +
                                                "The following keys have incompatible types: " +
                                                StringJoining.join(keysWithIncompatibleTypes)
                );
            }

            return userDefinedPropertySchemas;
        }
    }

    private static class Lazy extends NodeLabelTokenToPropertyKeys {

        private final Map<NodeLabelToken, Set<String>> labelToPropertyKeys;

        Lazy() {
            this.labelToPropertyKeys = new HashMap<>();
        }

        @Override
        void add(NodeLabelToken nodeLabelToken, Iterable<String> propertyKeys) {
            this.labelToPropertyKeys.compute(nodeLabelToken, (token, propertyKeySet) -> {
                var keys = (propertyKeySet == null) ? new HashSet<String>() : propertyKeySet;
                propertyKeys.forEach(keys::add);
                return keys;
            });
        }

        @Override
        Set<NodeLabel> nodeLabels() {
            return labelToPropertyKeys
                .keySet()
                .stream()
                .map(nodeLabelToken -> nodeLabelToken.isEmpty() ? NodeLabelTokens.ofNodeLabel(NodeLabel.ALL_NODES) : nodeLabelToken)
                .flatMap(NodeLabelToken::nodeLabels)
                .collect(Collectors.toSet());
        }

        @Override
        Map<String, PropertySchema> propertySchemas(
            NodeLabel nodeLabel,
            Map<String, PropertySchema> importPropertySchemas
        ) {
            return labelToPropertyKeys.keySet().stream()
                .filter(nodeLabelToken -> {
                    if (nodeLabelToken.isEmpty() && nodeLabel == NodeLabel.ALL_NODES) {
                        return true;
                    }
                    var nodeLabelTokenCount = nodeLabelToken.size();
                    for (int i = 0; i < nodeLabelTokenCount; i++) {
                        if (nodeLabelToken.get(i).equals(nodeLabel)) {
                            return true;
                        }
                    }
                    return false;
                })
                .flatMap(nodeLabelToken -> this.labelToPropertyKeys.get(nodeLabelToken).stream())
                .collect(Collectors.toMap(
                    propertyKey -> propertyKey,
                    importPropertySchemas::get,
                    (lhs, rhs) -> lhs
                ));
        }
    }
}
