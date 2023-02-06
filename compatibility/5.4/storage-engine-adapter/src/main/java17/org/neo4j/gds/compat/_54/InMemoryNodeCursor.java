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
package org.neo4j.gds.compat._54;

import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.compat.AbstractInMemoryNodeCursor;
import org.neo4j.storageengine.api.AllNodeScan;
import org.neo4j.storageengine.api.Degrees;
import org.neo4j.storageengine.api.LongReference;
import org.neo4j.storageengine.api.PropertySelection;
import org.neo4j.storageengine.api.Reference;
import org.neo4j.storageengine.api.RelationshipSelection;
import org.neo4j.storageengine.api.StoragePropertyCursor;
import org.neo4j.storageengine.api.StorageRelationshipTraversalCursor;
import org.neo4j.token.TokenHolders;

public class InMemoryNodeCursor extends AbstractInMemoryNodeCursor {

    public InMemoryNodeCursor(GraphStore graphStore, TokenHolders tokenHolders) {
        super(graphStore, tokenHolders);
    }

    @Override
    public boolean hasLabel() {
        return hasAtLeastOneLabelForCurrentNode();
    }

    @Override
    public Reference propertiesReference() {
        return LongReference.longReference(getId());
    }

    @Override
    public void properties(StoragePropertyCursor propertyCursor, PropertySelection selection) {
        propertyCursor.initNodeProperties(propertiesReference(), selection);
    }

    @Override
    public void properties(StoragePropertyCursor propertyCursor) {
        properties(propertyCursor, PropertySelection.ALL_PROPERTIES);
    }

    @Override
    public boolean supportsFastRelationshipsTo() {
        return false;
    }

    @Override
    public void relationshipsTo(
        StorageRelationshipTraversalCursor storageRelationshipTraversalCursor,
        RelationshipSelection relationshipSelection,
        long neighbourNodeReference
    ) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void degrees(RelationshipSelection selection, Degrees.Mutator mutator) {
    }

    @Override
    public boolean scanBatch(AllNodeScan allNodeScan, long sizeHint) {
        return super.scanBatch(allNodeScan, (int) sizeHint);
    }
}
