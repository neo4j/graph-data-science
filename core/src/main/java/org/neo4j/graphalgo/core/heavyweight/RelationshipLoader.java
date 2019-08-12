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

    /**
     * Loads the adjacency list for the given {@code sourceNode} and stores it
     * under the given {@code localNodeId}.
     *
     * The returned long value is a packed representation of the imported and the stored degree values
     * using {@link #combineDegrees(int, int)}. See that methods documentation for more explanation.
     */
    abstract long load(NodeCursor sourceNode, int localNodeId);

    /**
     * Combine two degree values, for some direction, into a single long value.
     * The first value should be the actual degree as reported by Neo4j,
     * i.e. the number of relationships that need to be loaded from the store
     * and processed during importing.
     *
     * The second value should be the degree value that will be reported by
     * the eventually loaded graph, i.e. the number relationships that we actually
     * store and could iterate over.
     *
     * In most cases the latter value, the algo degree, will be equal to the Neo4j degree.
     * Since we don't support multiple relationships to the same target node, might drop
     * such duplicated relationships, e.g. if we're loading with the requirement that
     * the final adjacency list will be sorted –
     * ({@link org.neo4j.graphalgo.core.GraphLoader#sorted()} is called with {@code true} as its argument.
     * In those cases, the algo degree can be less than the Neo4j degree.
     *
     * We eventually require both values, the first for progress logging (how many relationships were processed),
     * the second for providing information on the final graph size (how many relationships can we process).
     *
     * @see RawValues#combineIntInt(int, int)
     */
    static long combineDegrees(int originalDegree, int graphDegree) {
        return RawValues.combineIntInt(originalDegree, graphDegree);
    }

    /**
     * Adds two degrees together, that have been combined using {@link #combineDegrees(int, int)}.
     * The values are split into their components, summed separately and combined again.
     */
    static long addCombinedDegrees(long first, long second) {
        // NOTE: might that just `first + second` would work as well
        return combineDegrees(
                graphDegree(first) + algoDegree(second),
                graphDegree(first) + algoDegree(second)
        );
    }

    /**
     * @see #combineDegrees(int, int)
     * @see RawValues#getHead(long)
     */
    static int graphDegree(long combinedDegrees) {
        return RawValues.getHead(combinedDegrees);
    }

    /**
     * @see #combineDegrees(int, int)
     * @see RawValues#getTail(long)
     */
    static int algoDegree(long combinedDegrees) {
        return RawValues.getTail(combinedDegrees);
    }

    long readOutgoing(VisitRelationship visit, NodeCursor sourceNode, int localNodeId) {
        int outDegree = loadRelationships.degreeOut(sourceNode);
        if (outDegree == 0) {
            return outDegree;
        }
        final int[] targets = matrix.armOut(localNodeId, outDegree);
        final float[] weights = matrix.getOutWeights(localNodeId);
        visit.prepareNextNode(localNodeId, targets, weights);
        visitOut(sourceNode, visit);
        int storedOutDegree = visit.flush();
        matrix.setOutDegree(localNodeId, storedOutDegree);
        return combineDegrees(outDegree, storedOutDegree);
    }

    long readIncoming(VisitRelationship visit, NodeCursor sourceNode, int localNodeId) {
        final int inDegree = loadRelationships.degreeIn(sourceNode);
        if (inDegree == 0) {
            return inDegree;
        }
        final int[] targets = matrix.armIn(localNodeId, inDegree);
        final float[] weights = matrix.getInWeights(localNodeId);
        visit.prepareNextNode(localNodeId, targets, weights);
        visitIn(sourceNode, visit);
        int storedInDegree = visit.flush();
        matrix.setInDegree(localNodeId, storedInDegree);
        return combineDegrees(inDegree, storedInDegree);
    }

    long readUndirected(
            VisitRelationship visitOut,
            VisitRelationship visitIn,
            NodeCursor sourceNode,
            int localNodeId) {
        final int degree = loadRelationships.degreeUndirected(sourceNode);
        if (degree == 0) {
            return degree;
        }
        final int[] targets = matrix.armOut(localNodeId, degree);
        float[] weights = matrix.getOutWeights(localNodeId);
        visitIn.prepareNextNode(localNodeId, targets, weights);
        this.visitIn(sourceNode, visitIn);
        visitOut.prepareNextNode(visitIn);
        this.visitOut(sourceNode, visitOut);
        int storedDegree = visitOut.flush();
        matrix.setOutDegree(localNodeId, storedDegree);
        return combineDegrees(degree, storedDegree);
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

    static final class ReadNothing extends RelationshipLoader {
        ReadNothing(
                final KernelTransaction transaction,
                final AdjacencyMatrix matrix,
                final int[] relationType) {
            super(transaction, matrix, relationType);
        }

        @Override
        long load(NodeCursor sourceNode, int localNodeId) {
            return 0;
        }
    }

    static final class ReadOutgoing extends RelationshipLoader {
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
        long load(NodeCursor sourceNode, int localNodeId) {
            return readOutgoing(visitOutgoing, sourceNode, localNodeId);
        }
    }

    static final class ReadIncoming extends RelationshipLoader {
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
        long load(NodeCursor sourceNode, int localNodeId) {
            return readIncoming(visitIncoming, sourceNode, localNodeId);
        }
    }

    static final class ReadBoth extends RelationshipLoader {
        private final VisitRelationship visitOutgoing;
        private final VisitRelationship visitIncoming;

        ReadBoth(final ReadOutgoing readOut, final VisitRelationship visitIncoming) {
            super(readOut);
            this.visitOutgoing = readOut.visitOutgoing;
            this.visitIncoming = visitIncoming;
        }

        @Override
        long load(NodeCursor sourceNode, int localNodeId) {
            long out = readOutgoing(visitOutgoing, sourceNode, localNodeId);
            long in = readIncoming(visitIncoming, sourceNode, localNodeId);
            return addCombinedDegrees(out, in);
        }
    }

    static final class ReadUndirected extends RelationshipLoader {
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
        long load(NodeCursor sourceNode, int localNodeId) {
            return readUndirected(visitOutgoing, visitIncoming, sourceNode, localNodeId);
        }
    }

    static final class ReadWithNodeProperties extends RelationshipLoader {
        private final RelationshipLoader loader;
        private final WeightMap[] nodeProperties;

        ReadWithNodeProperties(
                final RelationshipLoader loader,
                final WeightMap... nodeProperties) {
            super(loader);
            this.loader = loader;
            this.nodeProperties = nodeProperties;
        }

        @Override
        long load(NodeCursor sourceNode, int localNodeId) {
            long imported = loader.load(sourceNode, localNodeId);
            for (WeightMap nodeProperty : nodeProperties) {
                readNodeWeight(sourceNode, localNodeId, nodeProperty, nodeProperty.propertyId());
            }
            return imported;
        }
    }
}