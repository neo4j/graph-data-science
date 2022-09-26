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
package org.neo4j.gds.compat._43;

import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.compat.AbstractInMemoryNodeCursor;
import org.neo4j.storageengine.api.Degrees;
import org.neo4j.storageengine.api.RelationshipSelection;
import org.neo4j.storageengine.api.StoragePropertyCursor;
import org.neo4j.token.TokenHolders;

public class InMemoryNodeCursor extends AbstractInMemoryNodeCursor {

    public InMemoryNodeCursor(GraphStore graphStore, TokenHolders tokenHolders) {
        super(graphStore, tokenHolders);
    }

    @Override
    public long propertiesReference() {
        return getId();
    }

    @Override
    public void properties(StoragePropertyCursor propertyCursor) {
        propertyCursor.initNodeProperties(propertiesReference());
    }

    @Override
    public void degrees(
        RelationshipSelection selection, Degrees.Mutator mutator, boolean allowFastDegreeLookup
    ) {
    }

}
