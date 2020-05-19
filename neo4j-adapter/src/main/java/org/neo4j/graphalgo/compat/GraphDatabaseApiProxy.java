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
package org.neo4j.graphalgo.compat;

import org.neo4j.common.DependencyResolver;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.internal.Version;

import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public final class GraphDatabaseApiProxy {

    public enum Neo4jVersion {
        V_4_0,
        V_4_1,
        UNKNOWN;



        public static Neo4jVersion parse(CharSequence version) {
            var majorVersion = Pattern.compile("\\.")
                .splitAsStream(version)
                .limit(2)
                .collect(Collectors.joining("."));
            switch (majorVersion) {
                case "4.0":
                    return V_4_0;
                case "4.1":
                    return V_4_1;
                default:
                    return UNKNOWN;
            }
        }

        @Override
        public String toString() {
            switch (this) {
                case V_4_0:
                    return "4.0";
                case V_4_1:
                    return "4.1";
                case UNKNOWN:
                    return "Unknown";
                default:
                    throw new IllegalArgumentException("Unexpected value: " + this + " (sad java 😞)");
            }
        }
    }

    public static Neo4jVersion neo4jVersion() {
        var neo4jVersion = Version.getNeo4jVersion();
        return Neo4jVersion.parse(neo4jVersion);
    }

    public static <T> T resolveDependency(GraphDatabaseService db, Class<T> dependency) {
        return ((GraphDatabaseAPI) db)
            .getDependencyResolver()
            .resolveDependency(dependency, DependencyResolver.SelectionStrategy.SINGLE);
    }

    public static void registerProcedures(GraphDatabaseService db, Class<?>... procedureClasses) throws Exception {
        GlobalProcedures procedures = resolveDependency(db, GlobalProcedures.class);
        for (Class<?> clazz : procedureClasses) {
            procedures.registerProcedure(clazz);
        }
    }

    public static void registerFunctions(GraphDatabaseService db, Class<?>... functionClasses) throws Exception {
        GlobalProcedures procedures = resolveDependency(db, GlobalProcedures.class);
        for (Class<?> clazz : functionClasses) {
            procedures.registerFunction(clazz);
        }
    }

    public static void registerAggregationFunctions(GraphDatabaseService db, Class<?>... functionClasses) throws
        Exception {
        GlobalProcedures procedures = resolveDependency(db, GlobalProcedures.class);
        for (Class<?> clazz : functionClasses) {
            procedures.registerAggregationFunction(clazz);
        }
    }

    public static Node getNodeById(Transaction tx, long id) {
        try {
            return tx.getNodeById(id);
        } catch (NotFoundException e) {
            return null;
        }
    }

    public static Node getNodeById(KernelTransaction tx, long id) {
        return getNodeById(tx.internalTransaction(), id);
    }

    public static NeoStores neoStores(GraphDatabaseService db) {
        return resolveDependency(db, RecordStorageEngine.class).testAccessNeoStores();
    }

    public static Result runQueryWithoutClosingTheResult(Transaction tx, String query, Map<String, Object> params) {
        return tx.execute(query, params);
    }

    public static Result runQueryWithoutClosingTheResult(
        KernelTransaction tx,
        String query,
        Map<String, Object> params
    ) {
        return tx.internalTransaction().execute(query, params);
    }

    public static void runInTransaction(GraphDatabaseService db, Consumer<Transaction> block) {
        try (Transaction tx = db.beginTx()) {
            block.accept(tx);
            tx.commit();
        }
    }

    public static <T> T applyInTransaction(GraphDatabaseService db, Function<Transaction, T> block) {
        try (Transaction tx = db.beginTx()) {
            T returnValue = block.apply(tx);
            tx.commit();
            return returnValue;
        }
    }

    public static Transactions newKernelTransaction(GraphDatabaseService db) {
        Transaction tx = db.beginTx();
        KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();
        return ImmutableTransactions.of(true, tx, ktx, Optional.empty());
    }

    public static Transactions newKernelTransaction(GraphDatabaseService db, SecurityContext context) {
        Transaction tx = db.beginTx();
        KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();
        return ImmutableTransactions.of(true, tx, ktx, Optional.ofNullable(context).map(ktx::overrideWith));
    }

    @ValueClass
    public interface Transactions extends AutoCloseable {
        boolean txShouldBeClosed();

        Transaction tx();

        KernelTransaction ktx();

        Optional<KernelTransaction.Revertable> revertKtx();

        @Override
        default void close() {
            tx().commit();
            if (txShouldBeClosed()) {
                revertKtx().ifPresent(KernelTransaction.Revertable::close);
                try {
                    ktx().close();
                } catch (TransactionFailureException e) {
                    throw new RuntimeException(e);
                } finally {
                    tx().close();
                }
            }
        }
    }

    private GraphDatabaseApiProxy() {
        throw new UnsupportedOperationException("No instances");
    }
}
