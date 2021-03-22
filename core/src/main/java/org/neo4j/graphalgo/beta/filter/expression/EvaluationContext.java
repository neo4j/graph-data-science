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
package org.neo4j.graphalgo.beta.filter.expression;

import com.carrotsearch.hppc.ObjectIntMap;
import com.carrotsearch.hppc.ObjectIntScatterMap;
import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.api.DefaultValue;
import org.neo4j.graphalgo.api.GraphStore;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public abstract class EvaluationContext {

    abstract double getProperty(String propertyKey);

    abstract boolean hasLabelsOrTypes(List<String> labelsOrTypes);

    public static class NodeEvaluationContext extends EvaluationContext {

        private final GraphStore graphStore;
        private final AtomicReference<NodeLabel> labelForPropertyReference;

        private long nodeId;

        public NodeEvaluationContext(GraphStore graphStore) {
            this.graphStore = graphStore;
            labelForPropertyReference = new AtomicReference<>();
        }

        @Override
        double getProperty(String propertyKey) {
            labelForPropertyReference.set(null);

            graphStore.nodes().forEachNodeLabel(nodeId, (nodeLabel -> {
                if (graphStore.hasNodeProperty(nodeLabel, propertyKey)) {
                    labelForPropertyReference.set(nodeLabel);
                    return false;
                }
                return true;
            }));

            var labelForProperty = labelForPropertyReference.get();
            if (labelForProperty == null) {
                return DefaultValue.DOUBLE_DEFAULT_FALLBACK;
            } else {
                return graphStore.nodePropertyValues(labelForProperty, propertyKey).doubleValue(nodeId);
            }
        }

        @Override
        boolean hasLabelsOrTypes(List<String> labels) {
            boolean hasAllLabels = true;
            for (String label: labels) {
                hasAllLabels &= graphStore.nodes().hasLabel(nodeId, NodeLabel.of(label));
            }
            return hasAllLabels;
        }

        public void init(long nodeId) {
            this.nodeId = nodeId;
        }
    }

    public static class RelationshipEvaluationContext extends EvaluationContext {

        private String relType;

        private double[] properties;

        private final ObjectIntMap<String> propertyIndices;

        public RelationshipEvaluationContext(Map<String, Integer> propertyIndices) {
            this.propertyIndices = new ObjectIntScatterMap<>();
            propertyIndices.forEach(this.propertyIndices::put);
        }

        @Override
        double getProperty(String propertyKey) {
            return properties[propertyIndices.get(propertyKey)];
        }

        @Override
        boolean hasLabelsOrTypes(List<String> relTypes) {
            boolean hasAnyType = false;
            for (String relType : relTypes) {
                hasAnyType |= this.relType.equals(relType);
            }
            return hasAnyType;
        }

        public void init(String relType) {
            this.relType = relType;
            this.properties = null;
        }

        public void init(String relType, double[] properties) {
            this.relType = relType;
            this.properties = properties;
        }

    }
}
