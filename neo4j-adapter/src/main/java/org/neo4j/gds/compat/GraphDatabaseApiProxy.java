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

import org.jetbrains.annotations.TestOnly;
import org.neo4j.common.DependencyResolver;
import org.neo4j.exceptions.KernelException;
import org.neo4j.gds.annotation.SuppressForbidden;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.procedure.CallableProcedure;
import org.neo4j.kernel.api.procedure.CallableUserAggregationFunction;
import org.neo4j.kernel.api.procedure.CallableUserFunction;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.factory.DbmsInfo;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.StorageEngineFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;


public final class GraphDatabaseApiProxy {

    public static Neo4jVersion neo4jVersion() {
        return Neo4jVersion.findNeo4jVersion();
    }

    public static boolean containsDependency(GraphDatabaseService db, Class<?> dependency) {
        return containsDependency(cast(db).getDependencyResolver(), dependency);
    }

    public static boolean containsDependency(DependencyResolver resolver, Class<?> dependency) {
        return resolver.containsDependency(dependency);
    }

    public static <T> T resolveDependency(GraphDatabaseService db, Class<T> dependency) {
        return resolveDependency(cast(db).getDependencyResolver(), dependency);
    }

    public static <T> T resolveDependency(DependencyResolver resolver, Class<T> dependency) {
        return resolver.resolveDependency(dependency, DependencyResolver.SelectionStrategy.SINGLE);
    }

    public static DependencyResolver dependencyResolver(GraphDatabaseService db) {
        return cast(db).getDependencyResolver();
    }

    public static void registerProcedures(GraphDatabaseService db, Class<?>... procedureClasses) throws
        KernelException {
        registerProcedures(db, false, procedureClasses);
    }

    public static void registerProcedures(
        GraphDatabaseService db,
        boolean overrideCurrentImplementation,
        Class<?>... procedureClasses
    ) throws KernelException {
        GlobalProcedures procedures = resolveDependency(db, GlobalProcedures.class);
        for (Class<?> clazz : procedureClasses) {
            registerProcedures(procedures, clazz, overrideCurrentImplementation);
        }
    }

    public static void registerFunctions(GraphDatabaseService db, Class<?>... functionClasses) throws KernelException {
        GlobalProcedures procedures = resolveDependency(db, GlobalProcedures.class);
        for (Class<?> clazz : functionClasses) {
            procedures.registerFunction(clazz);
        }
    }

    public static void registerAggregationFunctions(GraphDatabaseService db, Class<?>... functionClasses) throws
        KernelException {
        GlobalProcedures procedures = resolveDependency(db, GlobalProcedures.class);
        for (Class<?> clazz : functionClasses) {
            procedures.registerAggregationFunction(clazz);
        }
    }

    @SuppressForbidden(reason = "We're not implementing CallableUserAggregationFunction, just passing it on")
    public static void register(GraphDatabaseService db, CallableUserAggregationFunction function) throws
        KernelException {
        GlobalProcedures procedures = resolveDependency(db, GlobalProcedures.class);
        register(procedures, function);
    }

    @SuppressForbidden(reason = "We're not implementing CallableUserAggregationFunction, just passing it on")
    public static void register(GlobalProcedures procedures, CallableUserAggregationFunction function) throws
        KernelException {
        register(procedures, CallableUserAggregationFunction.class, function);
    }

    public static void register(GlobalProcedures procedures, CallableUserFunction function) throws
        KernelException {
        register(procedures, CallableUserFunction.class, function);
    }

    @SuppressForbidden(reason = "We're not implementing CallableProcedure, just passing it on")
    public static void register(GlobalProcedures procedures, CallableProcedure procedure) throws
        KernelException {
        register(procedures, CallableProcedure.class, procedure);
    }

    public static NamedDatabaseId databaseId(GraphDatabaseService db) {
        return cast(db).databaseId();
    }

    public static boolean isUsingFrekiStorageEngine(DependencyResolver dependencyResolver) {
        boolean isFrekiStorageEngineFactory = false;

        try {
            var clazz = Class.forName(
                "com.neo4j.internal.freki.FrekiStorageEngineFactory",
                true,
                dependencyResolver.getClass().getClassLoader()
            );
            var field = clazz.getField("NAME");
            if (field.getType() == String.class) {
                isFrekiStorageEngineFactory = dependencyResolver
                    .resolveDependency(StorageEngineFactory.class)
                    .name()
                    .equals(field.get(null));
            }
        } catch (ClassNotFoundException | NoSuchFieldException | IllegalAccessException ignored) {
        }

        return isFrekiStorageEngineFactory;
    }

    @TestOnly
    // DatabaseLayout switched from a class to an interface from 5.0 to 5.1
    // It should not be used in code paths run in production. Tests are ok
    public static DatabaseLayout databaseLayout(GraphDatabaseService db) {
        return cast(db).databaseLayout();
    }

    public static DbmsInfo dbmsInfo(GraphDatabaseService db) {
        return cast(db).dbmsInfo();
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

    public static KernelTransaction kernelTransaction(Transaction tx) {
        return ((InternalTransaction) tx).kernelTransaction();
    }

    public static InternalTransaction beginTransaction(
        GraphDatabaseService db,
        KernelTransaction.Type type,
        LoginContext loginContext
    ) {
        return cast(db).beginTransaction(type, loginContext);
    }

    public static Transactions newKernelTransaction(GraphDatabaseService db) {
        Transaction tx = db.beginTx();
        return ImmutableTransactions.of(tx, kernelTransaction(tx));
    }

    @ValueClass
    public interface Transactions extends AutoCloseable {
        Transaction tx();

        KernelTransaction ktx();

        @Override
        default void close() {
            tx().commit();
            tx().close();
        }
    }

    private static GraphDatabaseAPI cast(GraphDatabaseService databaseService) {
        return (GraphDatabaseAPI) databaseService;
    }

    private GraphDatabaseApiProxy() {
        throw new UnsupportedOperationException("No instances");
    }

    private static void registerProcedures(
        GlobalProcedures globalProcedures,
        Class<?> procedureClass,
        boolean overrideCurrentImplementation
    ) {
        var globalProceduresClass = globalProcedures.getClass();

        var legacySignatureMethod = getMethod(globalProceduresClass, "registerProcedure", Class.class, boolean.class);
        var newSignatureMethod = getMethod(globalProceduresClass, "registerProcedure", Class.class);
        if (legacySignatureMethod.isPresent()) {
            invokeMethod(globalProcedures, legacySignatureMethod.get(), procedureClass, overrideCurrentImplementation);
        } else if (newSignatureMethod.isPresent()){
            invokeMethod(globalProcedures, newSignatureMethod.get(), procedureClass);
        } else {
            throw new RuntimeException("Could not find registerProcedure method");
        }
    }

    private static void register(GlobalProcedures globalProcedures, Class<?> clazz, Object obj) {
        var globalProceduresClass = globalProcedures.getClass();

        var legacySignatureMethod = getMethod(globalProceduresClass, "register", clazz, boolean.class);
        var newSignatureMethod = getMethod(globalProceduresClass, "register", clazz);
        if (legacySignatureMethod.isPresent()) {
            invokeMethod(globalProcedures, legacySignatureMethod.get(), obj, true);
        } else if (newSignatureMethod.isPresent()){
            invokeMethod(globalProcedures, newSignatureMethod.get(), obj);
        } else {
            throw new RuntimeException("Could not find registerProcedure method");
        }
    }

    private static Optional<Method> getMethod(Class<?> clazz, String name, Class<?>... parameterTypes) {
        try {
            return Optional.of(clazz.getMethod(name, parameterTypes));
        } catch (NoSuchMethodException e) {
            return Optional.empty();
        }
    }

    private static void invokeMethod(Object obj, Method method, Object... arguments) {
        try {
            method.invoke(obj, arguments);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }
}
