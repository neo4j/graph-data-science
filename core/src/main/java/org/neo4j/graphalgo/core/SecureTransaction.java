/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.core;

import org.jetbrains.annotations.Nullable;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.api.security.RestrictedAccessMode;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Optional;

public final class SecureTransaction implements AutoCloseable {

    public interface TxConsumer<E extends Exception> {

        void accept(Transaction tx, KernelTransaction ktx) throws E;
    }

    public interface TxFunction<T, E extends Exception> {

        T apply(Transaction tx, KernelTransaction ktx) throws E;
    }

    public static SecureTransaction of(GraphDatabaseService db) {
        return of(db, (Transaction) null);
    }

    public static SecureTransaction of(GraphDatabaseService db, @Nullable Transaction top) {
        return new SecureTransaction(
            db,
            (InternalTransaction) top,
            Optional
                .ofNullable((InternalTransaction) top)
                .map(InternalTransaction::securityContext)
                .orElse(SecurityContext.AUTH_DISABLED)
        );
    }

    public static SecureTransaction of(GraphDatabaseService db, SecurityContext securityContext) {
        return of(db, null, securityContext);
    }

    public static SecureTransaction of(GraphDatabaseService db, @Nullable Transaction transaction, SecurityContext securityContext) {
        return new SecureTransaction(db, (InternalTransaction) transaction, securityContext);
    }

    private final GraphDatabaseService db;
    private final @Nullable InternalTransaction topTx;
    private final SecurityContext securityContext;

    private SecureTransaction(
        GraphDatabaseService db,
        @Nullable InternalTransaction top,
        SecurityContext securityContext
    ) {
        this.db = db;
        this.topTx = top;
        this.securityContext = securityContext;
    }

    public <T, E extends Exception> T apply(TxFunction<T, E> block) throws E {
        Transaction tx = db.beginTx();
        KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();
        KernelTransaction.Revertable revertable = ktx.overrideWith(securityContext);
        try (tx; revertable) {
            var result = block.apply(tx, ktx);
            tx.commit();
            return result;
        }
    }

    public <E extends Exception> void accept(TxConsumer<E> block) throws E {
        Transaction tx = db.beginTx();
        KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();
        KernelTransaction.Revertable revertable = ktx.overrideWith(securityContext);
        try (tx; revertable) {
            block.accept(tx, ktx);
            tx.commit();
        }
    }

    public GraphDatabaseAPI db() {
        return (GraphDatabaseAPI) db;
    }

    public Optional<KernelTransaction> ktx() {
        return Optional.ofNullable(topTx).map(InternalTransaction::kernelTransaction);
    }

    public SecureTransaction withRestrictedAccess(AccessMode accessMode) {
        var restrictedMode = new RestrictedAccessMode(securityContext.mode(), accessMode);
        var newContext = securityContext.withMode(restrictedMode);
        return new SecureTransaction(db, topTx, newContext);
    }

    public SecureTransaction fork() {
        InternalTransaction tx = (InternalTransaction) db.beginTx();
        tx.kernelTransaction().overrideWith(securityContext);
        return new SecureTransaction(db, tx, securityContext);
    }

    @Override
    public void close() {
        if (topTx != null) {
            topTx.close();
        }
    }
}
