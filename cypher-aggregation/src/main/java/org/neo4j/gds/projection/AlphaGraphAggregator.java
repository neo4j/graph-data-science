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
package org.neo4j.gds.projection;

import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.core.loading.Capabilities.WriteMode;
import org.neo4j.gds.metrics.projections.ProjectionMetricsService;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.NoValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.virtual.MapValue;

// public is required for the Cypher runtime
@SuppressWarnings("WeakerAccess")
public class AlphaGraphAggregator extends GraphAggregator {

    AlphaGraphAggregator(
        DatabaseId databaseId,
        String username,
        WriteMode writeMode,
        ExecutingQueryProvider queryProvider,
        ProjectionMetricsService projectionMetricsService
    ) {
        super(databaseId, username, writeMode, queryProvider, projectionMetricsService);
    }

    @Override
    public void update(AnyValue[] input) throws ProcedureException {
        try {
            var nodesConfig = nodeConfigMap(input[3]);
            var relationshipsConfig = relationshipConfigMap(input[4]);
            AnyValue dataConfig = NoValue.NO_VALUE;
            dataConfig = mergeMaps(dataConfig, nodesConfig);
            dataConfig = mergeMaps(dataConfig, relationshipsConfig);
            super.projectNextRelationship(
                (TextValue) input[0],
                input[1],
                input[2],
                dataConfig,
                input[5],
                NoValue.NO_VALUE
            );
        } catch (Exception e) {
            throw new ProcedureException(
                Status.Procedure.ProcedureCallFailed,
                e,
                "Failed to invoke function `%s`: Caused by: %s",
                AlphaCypherAggregation.FUNCTION_NAME,
                e
            );
        }
    }

    private static AnyValue nodeConfigMap(AnyValue nodeConfig) {
        if (nodeConfig == NoValue.NO_VALUE) {
            return NoValue.NO_VALUE;
        }

        var config = (MapValue) nodeConfig;

        if (config.containsKey(SOURCE_NODE_LABELS) && !config.containsKey(TARGET_NODE_LABELS)) {
            config = config.updatedWith(TARGET_NODE_LABELS, NoValue.NO_VALUE);
        }
        if (config.containsKey(TARGET_NODE_LABELS) && !config.containsKey(SOURCE_NODE_LABELS)) {
            config = config.updatedWith(SOURCE_NODE_LABELS, NoValue.NO_VALUE);
        }

        if (config.containsKey(SOURCE_NODE_PROPERTIES) && !config.containsKey(TARGET_NODE_PROPERTIES)) {
            config = config.updatedWith(TARGET_NODE_PROPERTIES, NoValue.NO_VALUE);
        }
        if (config.containsKey(TARGET_NODE_PROPERTIES) && !config.containsKey(SOURCE_NODE_PROPERTIES)) {
            config = config.updatedWith(SOURCE_NODE_PROPERTIES, NoValue.NO_VALUE);
        }

        return config;
    }

    private static AnyValue relationshipConfigMap(AnyValue relationshipConfig) {
        if (relationshipConfig == NoValue.NO_VALUE) {
            return NoValue.NO_VALUE;
        }

        var config = (MapValue) relationshipConfig;

        if (config.containsKey(ALPHA_RELATIONSHIP_PROPERTIES) && !config.containsKey(RELATIONSHIP_PROPERTIES)) {
            return config
                .filter((key, value) -> !key.equals(ALPHA_RELATIONSHIP_PROPERTIES))
                .updatedWith(RELATIONSHIP_PROPERTIES, config.get(ALPHA_RELATIONSHIP_PROPERTIES));
        }

        return config;
    }

    private static AnyValue mergeMaps(AnyValue left, AnyValue right) {
        if (left == NoValue.NO_VALUE || ((MapValue) left).isEmpty()) {
            return right;
        }
        if (right == NoValue.NO_VALUE || ((MapValue) right).isEmpty()) {
            return left;
        }
        return ((MapValue) left).updatedWith((MapValue) right);
    }
}
