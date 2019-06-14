/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
package org.neo4j.graphalgo.core;

import org.neo4j.graphalgo.api.GraphSetup;
import org.neo4j.graphalgo.core.utils.StatementFunction;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.newapi.InternalReadOps;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

public final class GraphDimensionsReader extends StatementFunction<GraphDimensions> {
    private final GraphSetup setup;

    public GraphDimensionsReader(
            GraphDatabaseAPI api,
            GraphSetup setup) {
        super(api);
        this.setup = setup;
    }

    @Override
    public GraphDimensions apply(final KernelTransaction transaction) throws RuntimeException {
        TokenRead tokenRead = transaction.tokenRead();
        Read dataRead = transaction.dataRead();
        // TODO: if the label (and type and property) is not found, we default to all labels, which is probably not what we want
        final int labelId = setup.loadAnyLabel() ? Read.ANY_LABEL : tokenRead.nodeLabel(setup.startLabel);

        int[] relationId = null;

        if (!setup.loadAnyRelationshipType()) {
            int relId = tokenRead.relationshipType(setup.relationshipType);
            if (relId != TokenRead.NO_TOKEN) {
                relationId = new int[]{relId};
            }
        }
        final int relWeightId = propertyKey(
                tokenRead,
                setup.shouldLoadRelationshipWeight(),
                setup.relationWeightPropertyName);

        int[] nodePropIds;
        if (setup.nodePropertyMappings.length > 0) {
            nodePropIds = new int[setup.nodePropertyMappings.length];
            for (int i = 0; i < setup.nodePropertyMappings.length; i++) {
                String propertyKey = setup.nodePropertyMappings[i].propertyKey;
                nodePropIds[i] = propertyKey(tokenRead, propertyKey != null, propertyKey);
            }
        } else {
            nodePropIds = new int[0];
        }

        final int nodeWeightId = propertyKey(tokenRead, setup.shouldLoadNodeWeight(), setup.nodeWeightPropertyName);
        final int nodePropId = propertyKey(tokenRead, setup.shouldLoadNodeProperty(), setup.nodePropertyName);

        final long nodeCount = dataRead.countsForNode(labelId);
        final long allNodesCount = InternalReadOps.getHighestPossibleNodeCount(dataRead, api);
        final long maxRelCount = Math.max(
                dataRead.countsForRelationshipWithoutTxState(
                        labelId,
                        relationId == null ? Read.ANY_RELATIONSHIP_TYPE : relationId[0],
                        Read.ANY_LABEL
                ),
                dataRead.countsForRelationshipWithoutTxState(
                        Read.ANY_LABEL,
                        relationId == null ? Read.ANY_RELATIONSHIP_TYPE : relationId[0],
                        labelId
                )
        );
        return new GraphDimensions.Builder()
                .setNodeCount(nodeCount)
                .setAllNodesCount(allNodesCount)
                .setMaxRelCount(maxRelCount)
                .setLabelId(labelId)
                .setRelationId(relationId)
                .setRelWeightId(relWeightId)
                .setNodeWeightId(nodeWeightId)
                .setNodePropId(nodePropId)
                .setNodePropIds(nodePropIds)
                .build();
    }

    private int propertyKey(TokenRead tokenRead, boolean load, String propertyName) {
        return load ? tokenRead.propertyKey(propertyName) : TokenRead.NO_TOKEN;
    }

}
