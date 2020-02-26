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
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.impl.api.KernelTransactions;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class GraphDatabaseApiProxy {

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

    public static void registerAggregationFunctions(GraphDatabaseService db, Class<?>... functionClasses) throws Exception {
        GlobalProcedures procedures = resolveDependency(db, GlobalProcedures.class);
        for (Class<?> clazz : functionClasses) {
            procedures.registerAggregationFunction(clazz);
        }
    }

    public static <T> T getNodeById(GraphDatabaseService db, long id, Function<Optional<Node>, T> action) {
        return applyInTransaction(db, tx -> action.apply(Optional.ofNullable(getNodeById(db, tx, id))));
    }

    public static Node getNodeById(GraphDatabaseService db, Transaction tx, long id) {
        try {
            return tx.getNodeById(id);
        } catch (NotFoundException e) {
            return null;
        }
    }

    public static <T> T expectNodeById(GraphDatabaseService db, long id, Function<Node, T> action) {
        return applyInTransaction(db, tx -> action.apply(expectNodeById(db, tx, id)));
    }

    public static Node expectNodeById(GraphDatabaseService db, Transaction tx, long id) {
        return tx.getNodeById(id);
    }

    public static <T> T findNode(GraphDatabaseService db, Label label, String propertyKey, Object propertyValue, Function<Optional<Node>, T> action) {
        return applyInTransaction(db, tx -> action.apply(Optional.ofNullable(findNode(db, tx, label,  propertyKey, propertyValue))));
    }

    public static Node findNode(GraphDatabaseService db, Transaction tx, Label label, String propertyKey, Object propertyValue) {
        return tx.findNode(label, propertyKey, propertyValue);
    }

    public static <T> List<T> findNodes(GraphDatabaseService db, Label label, Function<Node, T> action) {
        return applyInTransaction(db, tx -> {
            try (ResourceIterator<Node> nodes = findNodes(db, tx, label)) {
                return nodes.stream().map(action).collect(Collectors.toList());
            }
        });
    }

    public static ResourceIterator<Node> findNodes(GraphDatabaseService db, Transaction tx, Label label) {
        return tx.findNodes(label);
    }

    public static void findNodes(GraphDatabaseService db, Label label, String propertyKey, Object propertyValue, Consumer<Node> action) {
        runInTransaction(db, tx -> {
            try (ResourceIterator<Node> nodes = findNodes(db, tx, label, propertyKey, propertyValue)) {
                while (nodes.hasNext()) {
                    action.accept(nodes.next());
                }
            }
        });
    }

    public static ResourceIterator<Node> findNodes(GraphDatabaseService db, Transaction tx, Label label, String propertyKey, Object propertyValue) {
        return tx.findNodes(label, propertyKey, propertyValue);
    }

    public static void getAllNodes(GraphDatabaseService db, Consumer<Node> action) {
        runInTransaction(db, tx -> {
            try (ResourceIterator<Node> nodes = getAllNodes(db, tx).iterator()) {
                while (nodes.hasNext()) {
                    action.accept(nodes.next());
                }
            }
        });
    }

    public static ResourceIterable<Node> getAllNodes(GraphDatabaseService db, Transaction tx) {
        return tx.getAllNodes();
    }

    public static <T> T createNode(GraphDatabaseService db, Function<Node, T> action) {
        return applyInTransaction(db, tx -> action.apply(createNode(db, tx)));
    }

    public static Node createNode(GraphDatabaseService db, Transaction tx) {
        return tx.createNode();
    }

    public static <T> T createNode(GraphDatabaseService db, Function<Node, T> action, Label... labels) {
        return applyInTransaction(db, tx -> action.apply(createNode(db, tx, labels)));
    }

    public static Node createNode(GraphDatabaseService db, Transaction tx, Label... labels) {
        return tx.createNode(labels);
    }

    public static <T> List<T> getAllRelationships(GraphDatabaseService db, Function<Relationship, T> action) {
        return applyInTransaction(db, tx -> {
            try (ResourceIterator<Relationship> nodes = getAllRelationships(db, tx).iterator()) {
                return nodes.stream().map(action).collect(Collectors.toList());
            }
        });
    }

    public static ResourceIterable<Relationship> getAllRelationships(GraphDatabaseService db, Transaction tx) {
        return tx.getAllRelationships();
    }

    public static NeoStores neoStores(GraphDatabaseService db) {
        return resolveDependency(db, RecordStorageEngine.class).testAccessNeoStores();
    }

    public static KernelTransaction newExplicitKernelTransaction(
        GraphDatabaseService db,
        long timeout,
        TimeUnit timeoutUnit
    ) {
        return GraphDatabaseApiProxy
            .resolveDependency(db, KernelTransactions.class)
            .newInstance(
                KernelTransaction.Type.explicit,
                LoginContext.AUTH_DISABLED,
                ClientConnectionInfo.EMBEDDED_CONNECTION,
                timeoutUnit.toMillis(timeout)
            );
    }

    public static ProcedureCallContext procedureCallContext(String... outputFieldNames) {
        return new ProcedureCallContext(outputFieldNames, false, "", false);
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

    // TODO: broken
    public static <T> T withKernelTransaction(GraphDatabaseService db, Function<KernelTransaction, T> block) {
        return applyInTransaction(db, tx -> {
            KernelTransaction kernelTransaction = resolveDependency(db, KernelTransaction.class);
            return block.apply(kernelTransaction);
        });
    }

    public static Result runQuery(GraphDatabaseService db, String query, Map<String, Object> params) {
        return applyInTransaction(db, tx -> runQuery(db, tx, query, params));
    }

    public static Result runQuery(GraphDatabaseService db, Transaction tx, String query, Map<String, Object> params) {
        return tx.execute(query, params);
    }

    private GraphDatabaseApiProxy() {
        throw new UnsupportedOperationException("No instances");
    }
}
