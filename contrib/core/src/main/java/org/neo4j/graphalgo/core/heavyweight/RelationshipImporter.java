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
package org.neo4j.graphalgo.core.heavyweight;

import com.carrotsearch.hppc.LongDoubleMap;
import com.carrotsearch.hppc.cursors.LongDoubleCursor;
import org.neo4j.collection.primitive.PrimitiveIntIterable;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveLongIterable;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphSetup;
import org.neo4j.graphalgo.api.WeightMapping;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.IntIdMap;
import org.neo4j.graphalgo.core.WeightMap;
import org.neo4j.graphalgo.core.utils.ImportProgress;
import org.neo4j.graphalgo.core.utils.StatementAction;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_PROPERTY_KEY;


final class RelationshipImporter extends StatementAction {

    private final PrimitiveIntIterable nodes;
    private final GraphSetup setup;
    private final ImportProgress progress;
    private final int[] relationId;
    private final int relWeightId;

    private final int nodeSize;
    private final int nodeOffset;

    private IntIdMap idMap;
    private AdjacencyMatrix matrix;

    private Map<String, WeightMapping> nodeProperties;

    RelationshipImporter(
            GraphDatabaseAPI api,
            GraphSetup setup,
            GraphDimensions dimensions,
            ImportProgress progress,
            int batchSize,
            int nodeOffset,
            IntIdMap idMap,
            AdjacencyMatrix matrix,
            PrimitiveIntIterable nodes,
            Map<String, Supplier<WeightMapping>> nodePropertiesSupplier) {
        super(api);
        this.matrix = matrix;
        this.nodeSize = Math.min(batchSize, idMap.size() - nodeOffset);
        this.nodeOffset = nodeOffset;
        this.progress = progress;
        this.idMap = idMap;
        this.nodes = nodes;
        this.setup = setup;
        this.relationId = dimensions.relationshipTypeId();
        this.relWeightId = dimensions.relWeightId();
        this.nodeProperties = new HashMap<>();
        nodePropertiesSupplier.forEach((key, value) -> this.nodeProperties.put(key, value.get()));
    }

    @Override
    public String threadName() {
        return String.format(
                "[Heavy] RelationshipImport (%d..%d)",
                nodeOffset,
                nodeOffset + nodeSize);
    }

    @Override
    public void accept(final KernelTransaction transaction) {
        final Read readOp = transaction.dataRead();
        CursorFactory cursors = transaction.cursors();
        final RelationshipLoader loader = prepare(transaction, readOp, cursors);
        PrimitiveIntIterator iterator = nodes.iterator();
        try (NodeCursor nodeCursor = cursors.allocateNodeCursor()) {
            while (iterator.hasNext()) {
                final int nodeId = iterator.next();
                final long sourceNodeId = idMap.toOriginalNodeId(nodeId);
                readOp.singleNode(sourceNodeId, nodeCursor);
                if (nodeCursor.next()) {
                    int imported = loader.load(nodeCursor, nodeId);
                    progress.relationshipsImported(imported);
                }
            }
        }
    }

    private RelationshipLoader prepare(
            final KernelTransaction transaction,
            final Read readOp,
            final CursorFactory cursors) {
        final RelationshipLoader loader;
        if (setup.loadAsUndirected) {
            loader = prepareUndirected(transaction, readOp, cursors);
        } else {
            loader = prepareDirected(transaction, readOp, cursors);
        }

        WeightMap[] weightMaps = nodeProperties.values().stream()
                .filter(prop -> prop instanceof WeightMap)
                .map(prop -> (WeightMap) prop)
                .toArray(WeightMap[]::new);

        return new ReadWithNodeProperties(loader, weightMaps);
    }

    private RelationshipLoader prepareDirected(
            final KernelTransaction transaction,
            final Read readOp,
            final CursorFactory cursors) {
        final boolean loadIncoming = setup.loadIncoming;
        final boolean loadOutgoing = setup.loadOutgoing;
        final boolean sort = setup.sort;
        final boolean shouldLoadWeights = relWeightId != NO_SUCH_PROPERTY_KEY;

        RelationshipLoader loader = null;
        if (loadOutgoing) {
            final VisitRelationship visitor;
            if (shouldLoadWeights) {
                visitor = new VisitOutgoingWithWeight(readOp, cursors, idMap, sort, relWeightId, setup.relationDefaultWeight);
            } else {
                visitor = new VisitOutgoingNoWeight(idMap, sort);
            }
            loader = new ReadOutgoing(transaction, matrix, relationId, visitor);
        }
        if (loadIncoming) {
            final VisitRelationship visitor;
            if (shouldLoadWeights) {
                visitor = new VisitIncomingWithWeight(readOp, cursors, idMap, sort, relWeightId, setup.relationDefaultWeight);
            } else {
                visitor = new VisitIncomingNoWeight(idMap, sort);
            }
            if (loader != null) {
                ReadOutgoing readOutgoing = (ReadOutgoing) loader;
                loader = new ReadBoth(readOutgoing, visitor);
            } else {
                loader = new ReadIncoming(transaction, matrix, relationId, visitor);
            }
        }
        if (loader == null) {
            loader = new ReadNothing(transaction, matrix, relationId);
        }
        return loader;
    }

    private RelationshipLoader prepareUndirected(
            final KernelTransaction transaction,
            final Read readOp,
            final CursorFactory cursors) {
        final VisitRelationship visitorIn;
        final VisitRelationship visitorOut;
        if (relWeightId != NO_SUCH_PROPERTY_KEY) {
            visitorIn = new VisitIncomingWithWeight(
                    readOp,
                    cursors,
                    idMap,
                    true,
                    relWeightId,
                    setup.relationDefaultWeight);
            visitorOut = new VisitOutgoingWithWeight(
                    readOp,
                    cursors,
                    idMap,
                    true,
                    relWeightId,
                    setup.relationDefaultWeight);
        } else {
            visitorIn = new VisitIncomingNoWeight(idMap, true);
            visitorOut = new VisitOutgoingNoWeight(idMap, true);
        }
        return new ReadUndirected(transaction, matrix, relationId, visitorOut, visitorIn);
    }

    Graph toGraph(final IntIdMap idMap, final AdjacencyMatrix matrix) {

        return new HeavyGraph(
                idMap,
                matrix,
                nodeProperties);
    }

    void writeInto(Map<String, WeightMapping> nodeProperties) {
        for (Map.Entry<String, WeightMapping> entry : this.nodeProperties.entrySet()) {
            combineMaps(nodeProperties.get(entry.getKey()), entry.getValue());
        }
    }

    void release() {
        this.idMap = null;
        this.matrix = null;
        this.nodeProperties = null;
    }

    private void combineMaps(WeightMapping global, WeightMapping local) {
        if (global instanceof WeightMap && local instanceof WeightMap) {
            WeightMap localWeights = (WeightMap) local;
            final LongDoubleMap localMap = localWeights.weights();
            WeightMap globalWeights = (WeightMap) global;
            final LongDoubleMap globalMap = globalWeights.weights();

            for (LongDoubleCursor cursor : localMap) {
                globalMap.put(cursor.key, cursor.value);
            }
        }
    }
}
