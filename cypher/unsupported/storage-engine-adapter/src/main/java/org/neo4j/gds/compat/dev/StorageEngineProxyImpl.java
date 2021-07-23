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
package org.neo4j.gds.compat.dev;

import org.neo4j.gds.compat.AbstractInMemoryStorageEngine;
import org.neo4j.gds.compat.InMemoryStorageEngineBuilder;
import org.neo4j.gds.compat.StorageEngineProxyApi;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.token.TokenHolders;

public class StorageEngineProxyImpl implements StorageEngineProxyApi {
    @Override
    public <ENGINE extends AbstractInMemoryStorageEngine, BUILDER extends InMemoryStorageEngineBuilder<ENGINE>> BUILDER inMemoryStorageEngineBuilder(
        DatabaseLayout databaseLayout, TokenHolders tokenHolders
    ) {
        throw cypherUnsupportedException();
    }

    private UnsupportedOperationException cypherUnsupportedException() {
        return new UnsupportedOperationException("Cypher is not supported for Neo4j versions <4.3.");
    }
}
