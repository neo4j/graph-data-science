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
import org.neo4j.gds.values.GdsValue;

import java.util.Map;

public interface NodesBuilderApi {
    void addNode(long originalId, NodeLabelToken nodeLabels);

    void addNode(long originalId, NodeLabelToken nodeLabels, PropertyValues properties);

    default void addNode(long originalId) {
        this.addNode(originalId, NodeLabelTokens.empty());
    }

    default void addNode(long originalId, NodeLabel... nodeLabels) {
        this.addNode(originalId, NodeLabelTokens.ofNodeLabels(nodeLabels));
    }

    default void addNode(long originalId, NodeLabel nodeLabel) {
        this.addNode(originalId, NodeLabelTokens.ofNodeLabel(nodeLabel));
    }

    default void addNode(long originalId, Map<String, GdsValue> properties) {
        this.addNode(originalId, properties, NodeLabelTokens.empty());
    }

    default void addNode(long originalId, Map<String, GdsValue> properties, NodeLabelToken nodeLabels) {
        this.addNode(originalId, nodeLabels, PropertyValues.of(properties));
    }

    default void addNode(long originalId, Map<String, GdsValue> properties, NodeLabel... nodeLabels) {
        this.addNode(originalId, properties, NodeLabelTokens.ofNodeLabels(nodeLabels));
    }

    default void addNode(long originalId, Map<String, GdsValue> properties, NodeLabel nodeLabel) {
        this.addNode(originalId, properties, NodeLabelTokens.ofNodeLabel(nodeLabel));
    }
}
