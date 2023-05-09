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
package org.neo4j.gds.compat._58;

import org.neo4j.gds.compat.AbstractInMemoryRelationshipPropertyCursor;
import org.neo4j.gds.core.cypher.CypherGraphStore;
import org.neo4j.gds.storageengine.InMemoryRelationshipCursor;
import org.neo4j.storageengine.api.LongReference;
import org.neo4j.storageengine.api.PropertySelection;
import org.neo4j.storageengine.api.Reference;
import org.neo4j.storageengine.api.StorageRelationshipCursor;
import org.neo4j.token.TokenHolders;

public class InMemoryRelationshipPropertyCursor extends AbstractInMemoryRelationshipPropertyCursor {

    InMemoryRelationshipPropertyCursor(CypherGraphStore graphStore, TokenHolders tokenHolders) {
        super(graphStore, tokenHolders);
    }

    @Override
    public void initNodeProperties(
        Reference reference, PropertySelection propertySelection, long ownerReference
    ) {

    }

    @Override
    public void initRelationshipProperties(
        Reference reference, PropertySelection propertySelection, long ownerReference
    ) {
        var relationshipId = ((LongReference) reference).id;
        var relationshipCursor = new InMemoryRelationshipScanCursor(graphStore, tokenHolders);
        relationshipCursor.single(relationshipId);
        relationshipCursor.next();
        relationshipCursor.properties(this, new InMemoryPropertySelectionImpl(propertySelection));
    }

    @Override
    public void initRelationshipProperties(StorageRelationshipCursor relationshipCursor, PropertySelection selection) {
        var inMemoryRelationshipCursor = (InMemoryRelationshipCursor) relationshipCursor;
        inMemoryRelationshipCursor.properties(this, selection);
    }
}
