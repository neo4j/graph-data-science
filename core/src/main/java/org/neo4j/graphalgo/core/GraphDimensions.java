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

import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.api.GraphSetup;
import org.neo4j.graphalgo.core.utils.StatementFunction;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.newapi.InternalReadOps;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

public final class GraphDimensions extends StatementFunction<GraphDimensions> {
    private final GraphSetup setup;

    private long nodeCount;
    private long allNodesCount;
    private long maxRelCount;
    private long allRelsCount;
    private int labelId;
    private int[] relationId;
    private int relWeightId;

    private int nodeWeightId;
    private int nodePropId;
    private int[] nodePropIds;

    public GraphDimensions(
            GraphDatabaseAPI api,
            GraphSetup setup) {
        super(api);
        this.setup = setup;
    }

    public long hugeNodeCount() {
        return nodeCount;
    }

    public long allNodesCount() {
        return allNodesCount;
    }

    public int nodeCount() {
        return Math.toIntExact(nodeCount);
    }

    public long maxRelCount() {
        return maxRelCount;
    }

    public long allRelsCount() {
        return allRelsCount;
    }

    public int labelId() {
        return labelId;
    }

    public int[] relationshipTypeId() {
        return relationId;
    }

    public int singleRelationshipTypeId() {
        return relationId == null ? Read.ANY_RELATIONSHIP_TYPE : relationId[0];
    }

    public int relWeightId() {
        return relWeightId;
    }

    public int nodeWeightId() {
        return nodeWeightId;
    }

    public int nodePropId() {
        return nodePropId;
    }

    public int nodePropertyKeyId(String type) {
        PropertyMapping[] mappings = setup.nodePropertyMappings;

        for (int i = 0; i < mappings.length; i++) {
            if (mappings[i].propertyName.equals(type)) {
                return nodePropIds[i];
            }
        }
        return -1;
    }

    public int nodePropertyKeyId(int mappingIndex) {
        if (mappingIndex < 0 || mappingIndex >= nodePropIds.length) {
            return TokenRead.NO_TOKEN;
        }
        return nodePropIds[mappingIndex];
    }

    public double nodePropertyDefaultValue(String type) {
        PropertyMapping[] mappings = setup.nodePropertyMappings;

        for (PropertyMapping mapping : mappings) {
            if (mapping.propertyName.equals(type)) {
                return mapping.defaultValue;
            }
        }
        return 0.0;
    }

    @Override
    public GraphDimensions apply(final KernelTransaction transaction) throws RuntimeException {
        TokenRead tokenRead = transaction.tokenRead();
        Read dataRead = transaction.dataRead();
        labelId = setup.loadAnyLabel() ? Read.ANY_LABEL : tokenRead.nodeLabel(setup.startLabel);
        if (!setup.loadAnyRelationshipType()) {
            int relId = tokenRead.relationshipType(setup.relationshipType);
            if (relId != TokenRead.NO_TOKEN) {
                relationId = new int[]{relId};
            }
        }
        relWeightId = propertyKey(tokenRead, setup.shouldLoadRelationshipWeight(), setup.relationWeightPropertyName);

        if(setup.nodePropertyMappings.length > 0) {
            nodePropIds = new int[setup.nodePropertyMappings.length];
            for (int i = 0; i < setup.nodePropertyMappings.length; i++) {
                String propertyKey = setup.nodePropertyMappings[i].propertyKey;
                nodePropIds[i] = propertyKey(tokenRead, propertyKey != null, propertyKey);
            }
        } else {
            nodePropIds = new int[0];
        }

        nodeWeightId = propertyKey(tokenRead, setup.shouldLoadNodeWeight(), setup.nodeWeightPropertyName);
        nodePropId = propertyKey(tokenRead, setup.shouldLoadNodeProperty(), setup.nodePropertyName);

        nodeCount = dataRead.countsForNode(labelId);
        allNodesCount = InternalReadOps.getHighestPossibleNodeCount(dataRead, api);
        maxRelCount = Math.max(
                dataRead.countsForRelationshipWithoutTxState(
                        labelId,
                        singleRelationshipTypeId(),
                        Read.ANY_LABEL
                ),
                dataRead.countsForRelationshipWithoutTxState(
                        Read.ANY_LABEL,
                        singleRelationshipTypeId(),
                        labelId
                )
        );
        allRelsCount = InternalReadOps.getHighestPossibleRelationshipCount(dataRead, api);
        return this;
    }

    private int propertyKey(TokenRead tokenRead, boolean load, String propertyName) {
        return load ? tokenRead.propertyKey(propertyName) : TokenRead.NO_TOKEN;
    }

}
