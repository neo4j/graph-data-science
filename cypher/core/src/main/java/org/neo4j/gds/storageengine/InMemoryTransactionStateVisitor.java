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
package org.neo4j.gds.storageengine;

import org.eclipse.collections.api.set.primitive.LongSet;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphalgo.api.GraphStore;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.storageengine.api.txstate.TxStateVisitor;
import org.neo4j.token.TokenHolders;

public class InMemoryTransactionStateVisitor extends TxStateVisitor.Adapter {

    private final GraphStore graphStore;
    private final TokenHolders tokenHolders;

    public InMemoryTransactionStateVisitor(GraphStore graphStore, TokenHolders tokenHolders) {
        this.graphStore = graphStore;
        this.tokenHolders = tokenHolders;
    }

    @Override
    public void visitCreatedNode(long id) {

    }

    @Override
    public void visitDeletedNode(long id) {

    }

    @Override
    public void visitNodeLabelChanges(
        long id, LongSet added, LongSet removed
    ) {
        added.forEach(labelId -> {
            var labelString = tokenHolders.labelGetName((int) labelId);
            // TODO: implement this on graph store
            // graphStore.addLabelToNode(id, NodeLabel.of(labelString));
        });
    }

    @Override
    public void visitAddedIndex(IndexDescriptor index) throws KernelException {

    }

    @Override
    public void visitRemovedIndex(IndexDescriptor index) {

    }

    @Override
    public void visitAddedConstraint(ConstraintDescriptor element) throws KernelException {

    }

    @Override
    public void visitRemovedConstraint(ConstraintDescriptor element) {

    }

    @Override
    public void visitCreatedLabelToken(long id, String labelString, boolean internal) {
        // TODO: implement this on graph store
        // graphStore.addNodeLabel(NodeLabel.of(labelString));
    }

    @Override
    public void visitCreatedPropertyKeyToken(long id, String name, boolean internal) {

    }

    @Override
    public void visitCreatedRelationshipTypeToken(long id, String name, boolean internal) {

    }

    @Override
    public void close() {
        super.close();
    }
}
