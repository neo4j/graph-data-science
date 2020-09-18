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

import java.util.Optional;

/**
 * Manage transactions by making sure that the correct {@link SecurityContext} is applied.
 */
public final class SecureTransaction implements AutoCloseable {

    public interface TxConsumer<E extends Exception> {

        /**
         * Run some code within a transaction.
         * @throws E
         */
        void accept(Transaction tx, KernelTransaction ktx) throws E;
    }

    public interface TxFunction<T, E extends Exception> {

        /**
         * Calculate some result within a new transaction.
         * @throws E
         */
        T apply(Transaction tx, KernelTransaction ktx) throws E;
    }

    /**
     * Creates a new {@code SecureTransaction} with {@link AccessMode.Static#FULL} access.
     * This mirrors the default behavior of creating new transactions with {@link GraphDatabaseService#beginTx()}.
     */
    public static SecureTransaction of(GraphDatabaseService db) {
        return of(db, (Transaction) null);
    }

    /**
     * Creates a new {@code SecureTransaction} with the same {@link SecurityContext} as the provided {@link Transaction}.
     * If this instance is {@link #close() closed}, the supplied transaction will be {@link Transaction#close() closed} as well.
     * <p>
     * The {@code Transaction} can be null, in which case this method behaves the same as {@link #of(GraphDatabaseService)}.
     */
    public static SecureTransaction of(GraphDatabaseService db, @Nullable Transaction top) {
        return ofEdition(
            db,
            (InternalTransaction) top,
            Optional
                .ofNullable((InternalTransaction) top)
                .map(InternalTransaction::securityContext)
                .orElse(SecurityContext.AUTH_DISABLED)
        );
    }

    /**
     * Creates a new {@code SecureTransaction} with the given {@link SecurityContext}.
     */
    public static SecureTransaction of(GraphDatabaseService db, SecurityContext securityContext) {
        return of(db, null, securityContext);
    }

    /**
     * Creates a new {@code SecureTransaction} with the given {@link SecurityContext}, ignoring the context within the {@code Transaction}.
     * If this instance is {@link #close() closed}, the supplied transaction will be {@link Transaction#close() closed} as well.
     * <p>
     * The {@code Transaction} can be null, in which case this method behaves the same as {@link #of(GraphDatabaseService, SecurityContext)}.
     */
    public static SecureTransaction of(GraphDatabaseService db, @Nullable Transaction transaction, SecurityContext securityContext) {
        return ofEdition(db, (InternalTransaction) transaction, securityContext);
    }

    private static SecureTransaction ofEdition(
        GraphDatabaseService db,
        @Nullable InternalTransaction top,
        SecurityContext securityContext
    ) {
        securityContext = GdsEdition.instance().isOnEnterpriseEdition() ? securityContext : SecurityContext.AUTH_DISABLED;
        return new SecureTransaction(db, top, securityContext);
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

    /**
     * Run some code within a <strong>new</strong> {@code Transaction} under the managed {@code SecurityContext}.
     * The new transaction is closed afterwards and any resource that is tied to the lifecycle of that transaction
     * will throw a {@link org.neo4j.graphdb.NotInTransactionException} upon access.
     */
    public <T, E extends Exception> T apply(TxFunction<T, E> block) throws E {
        Transaction tx = db.beginTx();
        KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();
        ktx.overrideWith(securityContext);
        try (tx) {
            var result = block.apply(tx, ktx);
            tx.commit();
            return result;
        }
    }

    /**
     * Run some code within a <strong>new</strong> {@code Transaction} under the managed {@code SecurityContext}.
     * The new transaction is closed afterwards.
     */
    public <E extends Exception> void accept(TxConsumer<E> block) throws E {
        Transaction tx = db.beginTx();
        KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();
        ktx.overrideWith(securityContext);
        try (tx) {
            block.accept(tx, ktx);
            tx.commit();
        }
    }

    /**
     * Returns the {@link GraphDatabaseService} provided to this instance.
     */
    public GraphDatabaseService db() {
        return db;
    }

    /**
     * Returns the {@link KernelTransaction} for the provided top-level {@code Transaction}.
     * This is only present if this instance was created with either {@link #of(GraphDatabaseService, Transaction)}
     * or {@link #of(GraphDatabaseService, Transaction, SecurityContext)} with a non-{@code null} value for
     * the {@code Transaction} parameter.
     */
    public Optional<KernelTransaction> topLevelKernelTransaction() {
        return Optional.ofNullable(topTx).map(InternalTransaction::kernelTransaction);
    }

    /**
     * Returns a <strong>new</strong> {@link SecureTransaction} restricted by the provided {@link AccessMode}.
     * The mode only restricts does not override the given {@code SecurityContext}, i.e. you cannot grant more access.
     * <p>
     * One use-case is to restrict the access to {@link AccessMode.Static#READ} to make sure that only read-only
     * queries can be executed.
     * <p>
     * A new instance is returned, {@code this} instance remains untouched. However, the new instance has ownership
     * of the same top-level {@code Transaction} within {@code this} instance. Closing it would also close {@code this}
     * instance's top-level transaction. To decouple the transaction, call {@link #fork()} on the returned instance.
     */
    public SecureTransaction withRestrictedAccess(AccessMode.Static accessMode) {
        var restrictedMode = new RestrictedAccessMode(securityContext.mode(), accessMode);
        var newContext = securityContext.withMode(restrictedMode);
        return new SecureTransaction(db, topTx, newContext);
    }

    /**
     * Return a new {@link SecureTransaction} that owns a newly created top-level {@code Transaction}.
     * The returned instance will always own a transaction, regardless of whether {@code this} instance owns one.
     * The returned instance will operate under the same {@code SecurityContext} as {@code this} instance.
     * <p>
     * This is intended for when you need to keep track of a new transaction for a longer time, in which case you can
     * use {@link #topLevelKernelTransaction()} to get the underlying transaction. That method will never return an
     * empty {@code Optional} when it's called on the returned instance.
     */
    public SecureTransaction fork() {
        InternalTransaction tx = (InternalTransaction) db.beginTx();
        tx.kernelTransaction().overrideWith(securityContext);
        return new SecureTransaction(db, tx, securityContext);
    }

    /**
     * Closing this {@link SecureTransaction} only has an effect if it owns a top-level {@code Transaction},
     * in which case {@link Transaction#close()} will be called, without calling {@link Transaction#commit()}.
     */
    @Override
    public void close() {
        if (topTx != null) {
            topTx.close();
        }
    }
}
