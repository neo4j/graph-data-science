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
package org.neo4j.gds.compat;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.neo4j.gds.core.cypher.CypherGraphStore;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.storageengine.api.StoragePropertyCursor;
import org.neo4j.token.TokenHolders;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;

public abstract class AbstractInMemoryPropertyCursor
    extends PropertyRecord implements StoragePropertyCursor {

    protected final CypherGraphStore graphStore;
    protected final TokenHolders tokenHolders;
    @SuppressFBWarnings(
        value = "UWF_UNWRITTEN_PUBLIC_OR_PROTECTED_FIELD",
        justification = "Field will be initialized in the compat specific instances during initNodeProperties"
    )
    protected DelegatePropertyCursor delegate;

    public AbstractInMemoryPropertyCursor(CypherGraphStore graphStore, TokenHolders tokenHolders) {
        super(NO_ID);
        this.graphStore = graphStore;
        this.tokenHolders = tokenHolders;
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
        DelegatePropertyCursor(long id) {
            super(id);
        }
    }
}
