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

import org.neo4j.graphalgo.core.WeightMap;
import org.neo4j.graphalgo.core.loading.LoadRelationships;
import org.neo4j.graphalgo.core.loading.ReadHelper;
import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.helpers.RelationshipSelectionCursor;
import org.neo4j.kernel.api.KernelTransaction;

abstract class RelationshipLoader {
    private final KernelTransaction transaction;
    private final LoadRelationships loadRelationships;
    private final AdjacencyMatrix matrix;

    RelationshipLoader(
            final KernelTransaction transaction,
            final AdjacencyMatrix matrix,
            final int[] relationType) {
        this.transaction = transaction;
        this.matrix = matrix;
        loadRelationships = LoadRelationships.of(transaction.cursors(), relationType);
    }

    RelationshipLoader(final RelationshipLoader other) {
        this.transaction = other.transaction;
        this.matrix = other.matrix;
        this.loadRelationships = other.loadRelationships;
    }

    abstract int load(NodeCursor sourceNode, int localNodeId);

    int readOutgoing(VisitRelationship visit, NodeCursor sourceNode, int localNodeId) {
        int outDegree = loadRelationships.degreeOut(sourceNode);
        if (outDegree <= 0) {
            return outDegree;
        }
        final int[] targets = matrix.armOut(localNodeId, outDegree);
        final float[] weights = matrix.getOutWeights(localNodeId);
        visit.prepareNextNode(localNodeId, targets, weights);
        visitOut(sourceNode, visit);
        int finalOutDegree = visit.flush();
        matrix.setOutDegree(localNodeId, finalOutDegree);
        return finalOutDegree;
    }

    int readIncoming(VisitRelationship visit, NodeCursor sourceNode, int localNodeId) {
        final int inDegree = loadRelationships.degreeIn(sourceNode);
        if (inDegree <= 0) {
            return inDegree;
        }
        final int[] targets = matrix.armIn(localNodeId, inDegree);
        final float[] weights = matrix.getInWeights(localNodeId);
        visit.prepareNextNode(localNodeId, targets, weights);
        visitIn(sourceNode, visit);
        int finalInDegree = visit.flush();
        matrix.setInDegree(localNodeId, finalInDegree);
        return finalInDegree;
    }

    int readUndirected(
            VisitRelationship visitOut,
            VisitRelationship visitIn,
            NodeCursor sourceNode,
            int localNodeId) {
        final int degree = loadRelationships.degreeUndirected(sourceNode);
        if (degree <= 0) {
            return degree;
        }
        final int[] targets = matrix.armOut(localNodeId, degree);
        float[] weights = matrix.getOutWeights(localNodeId);
        visitIn.prepareNextNode(localNodeId, targets, weights);
        this.visitIn(sourceNode, visitIn);
        visitOut.prepareNextNode(visitIn);
        this.visitOut(sourceNode, visitOut);
        int finalDegree = visitOut.flush();
        matrix.setOutDegree(localNodeId, finalDegree);
        return finalDegree;
    }

    void readNodeWeight(
            NodeCursor sourceNode,
            int sourceGraphId,
            WeightMap weights,
            int propertyId) {
        try (PropertyCursor pc = transaction.cursors().allocatePropertyCursor()) {
            sourceNode.properties(pc);
            double weight = ReadHelper.readProperty(pc, propertyId, weights.defaultValue());
            if (weight != weights.defaultValue()) {
                weights.put(RawValues.combineIntInt(sourceGraphId, -1), weight);
            }
        }
    }

    private void visitOut(NodeCursor cursor, VisitRelationship visit) {
        visitRelationships(loadRelationships.relationshipsOut(cursor), visit);
    }

    private void visitIn(NodeCursor cursor, VisitRelationship visit) {
        visitRelationships(loadRelationships.relationshipsIn(cursor), visit);
    }

    private void visitRelationships(RelationshipSelectionCursor rels, VisitRelationship visit) {
        try (RelationshipSelectionCursor cursor = rels) {
            while (cursor.next()) {
                visit.visit(cursor);
            }
        }
    }
}

final class ReadNothing extends RelationshipLoader {
    ReadNothing(
            final KernelTransaction transaction,
            final AdjacencyMatrix matrix,
            final int[] relationType) {
        super(transaction, matrix, relationType);
    }

    @Override
    int load(NodeCursor sourceNode, int localNodeId) {
        return 0;
    }
}

final class ReadOutgoing extends RelationshipLoader {
    final VisitRelationship visitOutgoing;

    ReadOutgoing(
            final KernelTransaction transaction,
            final AdjacencyMatrix matrix,
            final int[] relationType,
            final VisitRelationship visitOutgoing) {
        super(transaction, matrix, relationType);
        this.visitOutgoing = visitOutgoing;
    }

    @Override
    int load(NodeCursor sourceNode, int localNodeId) {
        return readOutgoing(visitOutgoing, sourceNode, localNodeId);
    }
}

final class ReadIncoming extends RelationshipLoader {
    private final VisitRelationship visitIncoming;

    ReadIncoming(
            final KernelTransaction transaction,
            final AdjacencyMatrix matrix,
            final int[] relationType,
            final VisitRelationship visitIncoming) {
        super(transaction, matrix, relationType);
        this.visitIncoming = visitIncoming;
    }

    @Override
    int load(NodeCursor sourceNode, int localNodeId) {
        return readIncoming(visitIncoming, sourceNode, localNodeId);
    }
}

final class ReadBoth extends RelationshipLoader {
    private final VisitRelationship visitOutgoing;
    private final VisitRelationship visitIncoming;

    ReadBoth(final ReadOutgoing readOut, final VisitRelationship visitIncoming) {
        super(readOut);
        this.visitOutgoing = readOut.visitOutgoing;
        this.visitIncoming = visitIncoming;
    }

    @Override
    int load(NodeCursor sourceNode, int localNodeId) {
        return readOutgoing(visitOutgoing, sourceNode, localNodeId)
                + readIncoming(visitIncoming, sourceNode, localNodeId);
    }
}

final class ReadUndirected extends RelationshipLoader {
    private final VisitRelationship visitOutgoing;
    private final VisitRelationship visitIncoming;

    ReadUndirected(
            final KernelTransaction transaction,
            final AdjacencyMatrix matrix,
            final int[] relationType,
            final VisitRelationship visitOutgoing,
            final VisitRelationship visitIncoming) {
        super(transaction, matrix, relationType);
        this.visitOutgoing = visitOutgoing;
        this.visitIncoming = visitIncoming;
    }

    @Override
    int load(NodeCursor sourceNode, int localNodeId) {
        return readUndirected(visitOutgoing, visitIncoming, sourceNode, localNodeId);
    }
}

final class ReadWithNodeProperties extends RelationshipLoader {
    private final RelationshipLoader loader;
    private final WeightMap[] nodeProperties;

    ReadWithNodeProperties(
            final RelationshipLoader loader,
            final WeightMap...nodeProperties) {
        super(loader);
        this.loader = loader;
        this.nodeProperties = nodeProperties;
    }

    @Override
    int load(NodeCursor sourceNode, int localNodeId) {
        int imported = loader.load(sourceNode, localNodeId);
        for (WeightMap nodeProperty : nodeProperties) {
            readNodeWeight(sourceNode, localNodeId, nodeProperty, nodeProperty.propertyId());
        }
        return imported;
    }
}
