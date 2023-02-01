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
 * Manage transactions by making sure that the correct {@link org.neo4j.internal.kernel.api.security.SecurityContext} is applied.
 */
public final class TransactionContextImpl implements TransactionContext {

    /**
     * Creates a new {@code TransactionContext} with the same {@link org.neo4j.internal.kernel.api.security.SecurityContext} as the provided {@link org.neo4j.graphdb.Transaction}.
     */
    public static TransactionContext of(GraphDatabaseService databaseService, Transaction top) {
        if (top == null) {
            return of(databaseService, SecurityContext.AUTH_DISABLED);
        }
        var internalTransaction = (InternalTransaction) top;
        return of(databaseService, internalTransaction);
    }

    /**
     * Creates a new {@code TransactionContext} with the same {@link org.neo4j.internal.kernel.api.security.SecurityContext} as the provided {@link org.neo4j.kernel.impl.coreapi.InternalTransaction}.
     */
    public static TransactionContext of(GraphDatabaseService databaseService, InternalTransaction top) {
        return of(databaseService, top.securityContext());
    }

    /**
     * Creates a new {@code TransactionContext} with the provided {@link org.neo4j.internal.kernel.api.security.SecurityContext}.
     */
    public static TransactionContext of(GraphDatabaseService databaseService, SecurityContext securityContext) {
        return new TransactionContextImpl(databaseService, securityContext);
    }

    private final GraphDatabaseService databaseService;
    private final SecurityContext securityContext;

    private TransactionContextImpl(
        GraphDatabaseService databaseService,
        SecurityContext securityContext
    ) {
        this.databaseService = databaseService;
        this.securityContext = securityContext;
    }

    @Override
    public String username() {
        return Neo4jProxy.username(securityContext.subject());
    }

    @Override
    public boolean isGdsAdmin() {
        // this should be the same as the predefined role from enterprise-security
        // com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.ADMIN
        String PREDEFINED_ADMIN_ROLE = "admin";
        return securityContext.roles().contains(PREDEFINED_ADMIN_ROLE);
    }

    @Override
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

    @Override
    public <E extends Exception> void accept(TxConsumer<E> block) throws E {
        Transaction tx = databaseService.beginTx();
        KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();
        ktx.overrideWith(securityContext);
        try (tx) {
            block.accept(tx, ktx);
            tx.commit();
        }
    }

    @Override
    public TransactionContext withRestrictedAccess(AccessMode.Static accessMode) {
        var restrictedMode = new RestrictedAccessMode(securityContext.mode(), accessMode);
        var newContext = securityContext.withMode(restrictedMode);
        return new TransactionContextImpl(databaseService, newContext);
    }

    @Override
    public SecureTransaction fork() {
        InternalTransaction tx = (InternalTransaction) databaseService.beginTx();
        tx.kernelTransaction().overrideWith(securityContext);
        return new SecureTransaction(tx);
    }
}
