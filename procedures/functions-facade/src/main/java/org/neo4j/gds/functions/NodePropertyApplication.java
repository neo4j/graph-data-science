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
package org.neo4j.gds.functions;

import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.applications.algorithms.machinery.RequestScopedDependencies;
import org.neo4j.gds.core.loading.CatalogRequest;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.config.NodeIdParser;

import java.util.Objects;

import static java.util.Collections.singletonList;
import static org.neo4j.gds.ElementProjection.PROJECT_ALL;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;
import static org.neo4j.gds.utils.StringJoining.join;

class NodePropertyApplication {
    Object nodeProperty(RequestScopedDependencies requestScopedDependencies, String graphName, Object nodeId, String propertyKey, String nodeLabel) {
        Objects.requireNonNull(graphName);
        Objects.requireNonNull(nodeId);
        Objects.requireNonNull(propertyKey);
        Objects.requireNonNull(nodeLabel);

        var graphStore = GraphStoreCatalog.get(
            CatalogRequest.of(
                requestScopedDependencies.user(),
                requestScopedDependencies.databaseId()
            ), graphName
        ).graphStore();
        var projectAll = nodeLabel.equals(PROJECT_ALL);
        var nodeLabelType = projectAll ? NodeLabel.ALL_NODES : NodeLabel.of(nodeLabel);

        if (projectAll) {
            var labelsWithProperty = graphStore.nodeLabels().stream()
                .filter(label -> graphStore.hasNodeProperty(singletonList(label), propertyKey))
                .count();

            if (labelsWithProperty == 0) {
                throw new IllegalArgumentException(formatWithLocale(
                    "No node projection with property '%s' exists.",
                    propertyKey
                ));
            }
        } else {
            if (!graphStore.hasNodeProperty(singletonList(nodeLabelType), propertyKey)) {
                throw new IllegalArgumentException(formatWithLocale(
                    "Node projection '%s' does not have property key '%s'. Available keys: %s.",
                    nodeLabel,
                    propertyKey,
                    join(graphStore.nodePropertyKeys(NodeLabel.of(nodeLabel)))
                ));
            }
        }

        var neoNodeId = NodeIdParser.parseToSingleNodeId(nodeId, "nodeId");

        var internalId = graphStore.nodes().safeToMappedNodeId(neoNodeId);

        if (internalId == -1) {
            throw new IllegalArgumentException(formatWithLocale("Node id %d does not exist.", neoNodeId));
        }

        if (!projectAll && !graphStore.nodes().hasLabel(internalId, nodeLabelType)) {
            return null;
        }

        var propertyValues = graphStore.nodeProperty(propertyKey).values();

        switch (propertyValues.valueType()) {
            case LONG:
                var longValue = propertyValues.longValue(internalId);
                return longValue == DefaultValue.LONG_DEFAULT_FALLBACK ? DefaultValue.DOUBLE_DEFAULT_FALLBACK : (double) longValue;
            case DOUBLE:
                var propertyValue = propertyValues.doubleValue(internalId);
                return Double.isNaN(propertyValue) ? null : propertyValue;
            case DOUBLE_ARRAY:
                var doubleArray = propertyValues.doubleArrayValue(internalId);
                return doubleArray == null ? new double[] {} : doubleArray;
            case FLOAT_ARRAY:
                var floatArray = propertyValues.floatArrayValue(internalId);
                return floatArray == null ? new float[] {} : floatArray;
            case LONG_ARRAY:
                var longArray = propertyValues.longArrayValue(internalId);
                return longArray == null ? new long[] {} : longArray;
            case UNKNOWN:
        }

        throw new UnsupportedOperationException(formatWithLocale(
            "Cannot retrieve value from a property with type %s",
            propertyValues.valueType()
        ));
    }
}
