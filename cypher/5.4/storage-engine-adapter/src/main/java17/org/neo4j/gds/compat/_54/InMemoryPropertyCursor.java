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

import org.neo4j.gds.compat.AbstractInMemoryPropertyCursor;
import org.neo4j.gds.core.cypher.CypherGraphStore;
import org.neo4j.storageengine.api.PropertySelection;
import org.neo4j.storageengine.api.Reference;
import org.neo4j.storageengine.api.StorageNodeCursor;
import org.neo4j.storageengine.api.StorageRelationshipCursor;
import org.neo4j.token.TokenHolders;

public class InMemoryPropertyCursor extends AbstractInMemoryPropertyCursor {

    public InMemoryPropertyCursor(CypherGraphStore graphStore, TokenHolders tokenHolders) {
        super(graphStore, tokenHolders);
    }

    @Override
    public void initNodeProperties(Reference reference, PropertySelection selection, long ownerReference) {
        if (this.delegate == null || !(this.delegate instanceof InMemoryNodePropertyCursor)) {
            this.delegate = new InMemoryNodePropertyCursor(graphStore, tokenHolders);
        }

        ((InMemoryNodePropertyCursor) delegate).initNodeProperties(reference, selection);
    }

    @Override
    public void initNodeProperties(StorageNodeCursor nodeCursor, PropertySelection selection) {
        if (this.delegate == null || !(this.delegate instanceof InMemoryNodePropertyCursor)) {
            this.delegate = new InMemoryNodePropertyCursor(graphStore, tokenHolders);
        }

        ((InMemoryNodePropertyCursor) delegate).initNodeProperties(nodeCursor, selection);
    }

    @Override
    public void initRelationshipProperties(StorageRelationshipCursor relationshipCursor, PropertySelection selection) {
        if (this.delegate == null || !(this.delegate instanceof InMemoryRelationshipPropertyCursor)) {
            this.delegate = new InMemoryRelationshipPropertyCursor(graphStore, tokenHolders);
        }

        ((InMemoryRelationshipPropertyCursor) delegate).initRelationshipProperties(relationshipCursor, selection);
    }

    @Override
    public void initRelationshipProperties(Reference reference, PropertySelection selection, long ownerReference) {
        if (this.delegate == null || !(this.delegate instanceof InMemoryRelationshipPropertyCursor)) {
            this.delegate = new InMemoryRelationshipPropertyCursor(graphStore, tokenHolders);
        }

        ((InMemoryRelationshipPropertyCursor) delegate).initRelationshipProperties(reference, selection);
    }
}
