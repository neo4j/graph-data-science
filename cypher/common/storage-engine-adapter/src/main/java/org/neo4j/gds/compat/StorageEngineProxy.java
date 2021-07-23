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

import org.neo4j.counts.CountsStore;
import org.neo4j.graphalgo.api.GraphStore;
import org.neo4j.graphalgo.compat.GraphDatabaseApiProxy;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.token.TokenHolders;

import java.util.ServiceLoader;

public final class StorageEngineProxy {

    private static final StorageEngineProxyApi IMPL;

    static {
        var neo4jVersion = GraphDatabaseApiProxy.neo4jVersion();
        var storageEngineProxyFactory = ServiceLoader
            .load(StorageEngineProxyFactory.class)
            .stream()
            .map(ServiceLoader.Provider::get)
            .filter(f -> f.canLoad(neo4jVersion))
            .findFirst()
            .orElseThrow(() -> new LinkageError("Could not load the " + StorageEngineProxy.class + " implementation for " + neo4jVersion));
        IMPL = storageEngineProxyFactory.load();
    }

    private StorageEngineProxy() {}

    public static <ENGINE extends AbstractInMemoryStorageEngine, BUILDER extends InMemoryStorageEngineBuilder<ENGINE>> BUILDER inMemoryStorageEngineBuilder(
        DatabaseLayout databaseLayout,
        TokenHolders tokenHolders
    ) {
        return IMPL.inMemoryStorageEngineBuilder(databaseLayout, tokenHolders);
    }

    public static CountsStore inMemoryCountsStore(GraphStore graphStore, TokenHolders tokenHolders) {
        return IMPL.inMemoryCountsStore(graphStore, tokenHolders);
    }
}
