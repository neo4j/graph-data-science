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
import org.neo4j.function.TriFunction;
import org.neo4j.graphalgo.api.GraphStore;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.storageengine.api.CommandCreationContext;
import org.neo4j.storageengine.api.MetadataProvider;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.txstate.TxStateVisitor;
import org.neo4j.token.TokenHolders;

import java.util.function.BiFunction;
import java.util.function.Supplier;

public abstract class InMemoryStorageEngineBuilder<T extends AbstractInMemoryStorageEngine> {

    protected final DatabaseLayout databaseLayout;
    protected final TokenHolders tokenHolders;

    protected BiFunction<GraphStore, TokenHolders, CountsStore> countsStoreFn;
    protected BiFunction<GraphStore, TokenHolders, TxStateVisitor> txStateVisitorFn;
    protected MetadataProvider metadataProvider;
    protected Supplier<CommandCreationContext> commandCreationContextSupplier;
    protected TriFunction<GraphStore, TokenHolders, CountsStore, StorageReader> storageReaderFn;

    protected InMemoryStorageEngineBuilder(
        DatabaseLayout databaseLayout,
        TokenHolders tokenHolders
    ) {
        this.databaseLayout = databaseLayout;
        this.tokenHolders = tokenHolders;
    }

    public InMemoryStorageEngineBuilder<T> withCountsStoreFn(BiFunction<GraphStore, TokenHolders, CountsStore> countsStoreFn) {
        this.countsStoreFn = countsStoreFn;
        return this;
    }

    public InMemoryStorageEngineBuilder<T> withTxStateVisitorFn(BiFunction<GraphStore, TokenHolders, TxStateVisitor> txStateVisitorFn) {
        this.txStateVisitorFn = txStateVisitorFn;
        return this;
    }

    public InMemoryStorageEngineBuilder<T> withMetadataProvider(MetadataProvider metadataProvider) {
        this.metadataProvider = metadataProvider;
        return this;
    }

    public InMemoryStorageEngineBuilder<T> withCommandCreationContextSupplier(Supplier<CommandCreationContext> commandCreationContextSupplier) {
        this.commandCreationContextSupplier = commandCreationContextSupplier;
        return this;
    }

    public InMemoryStorageEngineBuilder<T> withStorageReaderFn(TriFunction<GraphStore, TokenHolders, CountsStore, StorageReader> storageReaderFn) {
        this.storageReaderFn = storageReaderFn;
        return this;
    }

    public abstract T build();
}
