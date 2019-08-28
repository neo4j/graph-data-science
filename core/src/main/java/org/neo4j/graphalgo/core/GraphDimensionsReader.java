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

import org.neo4j.graphalgo.KernelPropertyMapping;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.RelationshipTypeMapping;
import org.neo4j.graphalgo.api.GraphSetup;
import org.neo4j.graphalgo.core.utils.RelationshipTypes;
import org.neo4j.graphalgo.core.utils.StatementFunction;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.newapi.InternalReadOps;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Arrays;
import java.util.Set;

public final class GraphDimensionsReader extends StatementFunction<GraphDimensions> {
    private final GraphSetup setup;
    private final boolean readLabelAndType;

    public GraphDimensionsReader(
            GraphDatabaseAPI api,
            GraphSetup setup,
            boolean readLabelAndType) {
        super(api);
        this.setup = setup;
        this.readLabelAndType = readLabelAndType;
    }

    @Override
    public GraphDimensions apply(final KernelTransaction transaction) throws RuntimeException {
        TokenRead tokenRead = transaction.tokenRead();
        Read dataRead = transaction.dataRead();
        final int labelId = readLabelAndType && !setup.loadAnyLabel()
                ? tokenRead.nodeLabel(setup.startLabel)
                : Read.ANY_LABEL;

        RelationshipTypeMapping[] relationshipTypeMappings;
        if (readLabelAndType && !setup.loadAnyRelationshipType()) {
            Set<String> types = RelationshipTypes.parse(setup.relationshipType);
            relationshipTypeMappings = types.stream()
                    .map(name -> {
                        int relationshipType = tokenRead.relationshipType(name);
                        return new RelationshipTypeMapping(name, relationshipType);
                    })
                    .toArray(RelationshipTypeMapping[]::new);
        } else {
            relationshipTypeMappings = new RelationshipTypeMapping[0];
        }
        final int relWeightId = propertyKey(
                tokenRead,
                setup.shouldLoadRelationshipWeight(),
                setup.relationWeightPropertyName);

        PropertyMapping[] nodePropertyMappings = setup.nodePropertyMappings;
        KernelPropertyMapping[] nodeProperties = new KernelPropertyMapping[nodePropertyMappings.length];
        for (int i = 0; i < nodePropertyMappings.length; i++) {
            PropertyMapping propertyMapping = nodePropertyMappings[i];
            String propertyKey = nodePropertyMappings[i].propertyKey;
            int key = propertyKey(tokenRead, propertyKey != null, propertyKey);
            nodeProperties[i] = propertyMapping.toKernelMapping(key);
        }

        final long nodeCount = dataRead.countsForNode(labelId);
        final long allNodesCount = InternalReadOps.getHighestPossibleNodeCount(dataRead, api);
        final long maxRelCount = relationshipTypeMappings.length == 0
                ? maxRelCountForLabelAndType(dataRead, labelId, Read.ANY_RELATIONSHIP_TYPE)
                : Arrays.stream(relationshipTypeMappings)
                .mapToInt(mapping -> mapping.typeId)
                .mapToLong(id -> maxRelCountForLabelAndType(dataRead, labelId, id))
                .sum();

        return new GraphDimensions.Builder()
                .setNodeCount(nodeCount)
                .setHighestNeoId(allNodesCount)
                .setMaxRelCount(maxRelCount)
                .setLabelId(labelId)
                .setRelationshipTypeMappings(relationshipTypeMappings)
                .setRelWeightId(relWeightId)
                .setNodeProperties(nodeProperties)
                .build();
    }

    private static long maxRelCountForLabelAndType(Read dataRead, int labelId, int id) {
        return Math.max(
                dataRead.countsForRelationshipWithoutTxState(labelId, id, Read.ANY_LABEL),
                dataRead.countsForRelationshipWithoutTxState(Read.ANY_LABEL, id, labelId)
        );
    }

    private int propertyKey(TokenRead tokenRead, boolean load, String propertyName) {
        return load ? tokenRead.propertyKey(propertyName) : TokenRead.NO_TOKEN;
    }

}
