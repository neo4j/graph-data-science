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
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.Username;
import org.neo4j.gds.core.loading.CatalogRequest;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;
import org.neo4j.values.storable.DoubleArray;
import org.neo4j.values.storable.FloatArray;

import java.util.Objects;

import static java.util.Collections.singletonList;
import static org.neo4j.gds.ElementProjection.PROJECT_ALL;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;
import static org.neo4j.gds.utils.StringJoining.join;

public class NodePropertyFunc {
    @Context
    public GraphDatabaseAPI api;

    @Context
    public Username username = Username.EMPTY_USERNAME;

    @UserFunction("gds.util.nodeProperty")
    @Description("Returns a node property value from a named in-memory graph.")
    public Object nodeProperty(
        @Name(value = "graphName") String graphName,
        @Name(value = "nodeId") Number nodeId,
        @Name(value = "propertyKey") String propertyKey,
        @Name(value = "nodeLabel", defaultValue = "*") String nodeLabel
    ) {
        Objects.requireNonNull(graphName);
        Objects.requireNonNull(nodeId);
        Objects.requireNonNull(propertyKey);
        Objects.requireNonNull(nodeLabel);

        GraphStore graphStore = GraphStoreCatalog.get(CatalogRequest.of(username.username(), DatabaseId.of(api)), graphName).graphStore();
        boolean projectAll = nodeLabel.equals(PROJECT_ALL);
        var nodeLabelType = projectAll ? NodeLabel.ALL_NODES : NodeLabel.of(nodeLabel);

        if (projectAll) {
            long labelsWithProperty = graphStore.nodeLabels().stream()
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

        long internalId = graphStore.nodes().safeToMappedNodeId(nodeId.longValue());

        if (internalId == -1) {
            throw new IllegalArgumentException(formatWithLocale("Node id %d does not exist.", nodeId.longValue()));
        }

        if (!projectAll && !graphStore.nodes().hasLabel(internalId, nodeLabelType)) {
            return null;
        }

        var propertyValues = graphStore.nodeProperty(propertyKey).values();

        switch (propertyValues.valueType()) {
            case LONG:
                long longValue = propertyValues.longValue(internalId);
                return longValue == DefaultValue.LONG_DEFAULT_FALLBACK ? DefaultValue.DOUBLE_DEFAULT_FALLBACK : (double) longValue;
            case DOUBLE:
                double propertyValue = propertyValues.doubleValue(internalId);
                return Double.isNaN(propertyValue) ? null : propertyValue;
            case DOUBLE_ARRAY:
                double[] doubleArray = ((DoubleArray)propertyValues.value(internalId)).asObjectCopy();
                return doubleArray == null ? new double[] {} : doubleArray;
            case FLOAT_ARRAY:
                float[] floatArray = ((FloatArray)propertyValues.value(internalId)).asObjectCopy();
                return floatArray == null ? new float[] {} : floatArray;
            case LONG_ARRAY:
                long[] longArray = propertyValues.longArrayValue(internalId);
                return longArray == null ? new long[] {} : longArray;
            case UNKNOWN:
        }

        throw new UnsupportedOperationException(formatWithLocale(
            "Cannot retrieve value from a property with type %s",
            propertyValues.valueType()
        ));
    }
}
