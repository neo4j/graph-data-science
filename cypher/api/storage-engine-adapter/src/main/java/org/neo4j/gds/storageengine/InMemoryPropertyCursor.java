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

import org.neo4j.gds.api.GraphStore;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.storageengine.api.StoragePropertyCursor;
import org.neo4j.token.TokenHolders;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;

public class InMemoryPropertyCursor extends PropertyRecord implements StoragePropertyCursor {

    private final GraphStore graphStore;
    private final TokenHolders tokenHolders;
    private DelegatePropertyCursor delegate;

    public InMemoryPropertyCursor(GraphStore graphStore, TokenHolders tokenHolders) {
        super(NO_ID);
        this.graphStore = graphStore;
        this.tokenHolders = tokenHolders;
    }

    @Override
    public void initNodeProperties(long reference, long ownerReference) {
        this.delegate = new InMemoryNodePropertyCursor(graphStore, tokenHolders);
        this.delegate.initNodeProperties(reference);
    }

    @Override
    public void initRelationshipProperties(long reference, long ownerReference) {

    }

    @Override
    public void initRelationshipProperties(long reference) {

    }

    @Override
    public int propertyKey() {
        return delegate.propertyKey();
    }

    @Override
    public ValueGroup propertyType() {
        return delegate.propertyType();
    }

    @Override
    public Value propertyValue() {
        return delegate.propertyValue();
    }

    @Override
    public boolean next() {
        return delegate.next();
    }

    @Override
    public void reset() {
        delegate.reset();
    }

    @Override
    public void clear() {
        super.clear();
    }

    @Override
    public void setForceLoad() {
        delegate.setForceLoad();
    }

    @Override
    public void close() {
        if (delegate != null) {
            delegate.close();
        }
    }

    abstract static class DelegatePropertyCursor extends PropertyRecord implements StoragePropertyCursor {

        protected final GraphStore graphStore;
        protected final TokenHolders tokenHolders;

        DelegatePropertyCursor(long id, GraphStore graphStore, TokenHolders tokenHolders) {
            super(id);
            this.graphStore = graphStore;
            this.tokenHolders = tokenHolders;
        }
    }

}
