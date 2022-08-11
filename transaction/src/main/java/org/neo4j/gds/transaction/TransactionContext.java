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
package org.neo4j.gds.transaction;

import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.api.security.RestrictedAccessMode;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;

/**
 * Manage transactions by making sure that the correct {@link SecurityContext} is applied.
 */
public final class TransactionContext {

    public interface TxConsumer<E extends Exception> {

        /**
         * Run some code within a transaction.
         *
         * @throws E
         */
        void accept(Transaction tx, KernelTransaction ktx) throws E;
    }

    public interface TxFunction<T, E extends Exception> {

        /**
         * Calculate some result within a new transaction.
         *
         * @throws E
         */
        T apply(Transaction tx, KernelTransaction ktx) throws E;
    }

    /**
     * Creates a new {@code TransactionContext} with the same {@link SecurityContext} as the provided {@link Transaction}.
     */
    public static TransactionContext of(GraphDatabaseService databaseService, Transaction top) {
        var internalTransaction = (InternalTransaction) top;
        return of(databaseService, internalTransaction);
    }

    /**
     * Creates a new {@code TransactionContext} with the same {@link SecurityContext} as the provided {@link InternalTransaction}.
     */
    public static TransactionContext of(GraphDatabaseService databaseService, InternalTransaction top) {
        return of(databaseService, top.securityContext());
    }

    /**
     * Creates a new {@code TransactionContext} with the provided {@link SecurityContext}.
     */
    public static TransactionContext of(GraphDatabaseService databaseService, SecurityContext securityContext) {
        return new TransactionContext(databaseService, securityContext);
    }

    private final GraphDatabaseService databaseService;
    private final SecurityContext securityContext;

    private TransactionContext(
        GraphDatabaseService databaseService,
        SecurityContext securityContext
    ) {
        this.databaseService = databaseService;
        this.securityContext = securityContext;
    }

    /**
     * @return The username associated with the current {@link SecurityContext}.
     */
    public String username() {
        return Neo4jProxy.username(securityContext.subject());
    }

    /**
     * Run some code within a <strong>new</strong> {@code Transaction} under the managed {@code SecurityContext}.
     * The new transaction is closed afterwards and any resource that is tied to the lifecycle of that transaction
     * will throw a {@link org.neo4j.graphdb.NotInTransactionException} upon access.
     */
    public <T, E extends Exception> T apply(TxFunction<T, E> block) throws E {
        Transaction tx = databaseService.beginTx();
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
        Transaction tx = databaseService.beginTx();
        KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();
        ktx.overrideWith(securityContext);
        try (tx) {
            block.accept(tx, ktx);
            tx.commit();
        }
    }

    /**
     * Returns a <strong>new</strong> {@link TransactionContext} restricted by the provided {@link AccessMode}.
     * The mode only restricts but does not override the given {@code SecurityContext}, i.e. you cannot grant more access.
     * <p>
     * One use-case is to restrict the access to {@link AccessMode.Static#READ} to make sure that only read-only
     * queries can be executed.
     * <p>
     * A new instance is returned, {@code this} instance remains untouched.
     */
    public TransactionContext withRestrictedAccess(AccessMode.Static accessMode) {
        var restrictedMode = new RestrictedAccessMode(securityContext.mode(), accessMode);
        var newContext = securityContext.withMode(restrictedMode);
        return new TransactionContext(databaseService, newContext);
    }

    /**
     * Return a new {@link SecureTransaction} that owns a newly created top-level {@code Transaction}.
     * The returned instance will operate under the {@code SecurityContext} as provided by this {@code TransactionContext}.
     * <p>
     * For shorter tasks, consider using {@link #accept(TransactionContext.TxConsumer)} or {@link #apply(TransactionContext.TxFunction)}
     * which make sure that the created transaction is closed.
     * <p>
     * This is intended for when you need to keep track of a new transaction for a longer time, in which case you can
     * use {@link SecureTransaction#kernelTransaction()} to get the underlying transaction.
     */
    public SecureTransaction fork() {
        InternalTransaction tx = (InternalTransaction) databaseService.beginTx();
        tx.kernelTransaction().overrideWith(securityContext);
        return new SecureTransaction(tx);
    }

    public static final class SecureTransaction implements AutoCloseable {
        private final InternalTransaction tx;

        private SecureTransaction(InternalTransaction tx) {
            this.tx = tx;
        }

        /**
         * @return The current {@link org.neo4j.kernel.api.KernelTransaction}.
         */
        public KernelTransaction kernelTransaction() {
            return tx.kernelTransaction();
        }

        /**
         * Closes the underlying transaction.
         */
        public void close() {
            tx.close();
        }
    }
}
