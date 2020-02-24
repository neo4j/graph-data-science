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
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphalgo.QueryRunner;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.ResultTransformer;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.StoreId;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

public final class GraphDbApi implements GraphDatabaseAPI {
    private static final String DB_NAME = GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

    private final GraphDatabaseAPI api;
    private final DatabaseManagementService dbms;

    public GraphDbApi(DatabaseManagementService dbms) {
        this.api = (GraphDatabaseAPI) dbms.database(DB_NAME);
        this.dbms = dbms;
    }

    public void withinTransaction(Consumer<Transaction> action) {
        try (Transaction transaction = api.beginTx()) {
            action.accept(transaction);
        }
    }

    public <T> T withinTransactionApply(Function<Transaction, T> action) {
        try (Transaction transaction = api.beginTx()) {
            return action.apply(transaction);
        }
    }

    public void runQuery(String query) throws QueryExecutionException {
        QueryRunner.runQuery(api, query);
    }

    public void runQuery( String query, Consumer<Result.ResultRow> check ) throws QueryExecutionException {
        QueryRunner.runQueryWithRowConsumer(api, query, check);
    }

    public void runQuery( String query, Map<String,Object> parameters ) throws QueryExecutionException {
        QueryRunner.runQuery(api, query, parameters);
    }

    public void shutdown() {
        dbms.shutdownDatabase(DB_NAME);
        dbms.shutdown();
    }

    // delegate methods


    @Override
    public DependencyResolver getDependencyResolver() {
        return api.getDependencyResolver();
    }

    @Override
    public StoreId storeId() {
        return api.storeId();
    }

    @Override
    public DatabaseLayout databaseLayout() {
        return api.databaseLayout();
    }

    @Override
    public NamedDatabaseId databaseId() {
        return api.databaseId();
    }

    @Override
    public DatabaseInfo databaseInfo() {
        return api.databaseInfo();
    }

    @Override
    public InternalTransaction beginTransaction(
        KernelTransaction.Type type,
        LoginContext loginContext
    ) {
        return api.beginTransaction(type, loginContext);
    }

    @Override
    public InternalTransaction beginTransaction(
        KernelTransaction.Type type,
        LoginContext loginContext,
        ClientConnectionInfo clientInfo
    ) {
        return api.beginTransaction(type, loginContext, clientInfo);
    }

    @Override
    public InternalTransaction beginTransaction(
        KernelTransaction.Type type,
        LoginContext loginContext,
        ClientConnectionInfo clientInfo,
        long timeout,
        TimeUnit unit
    ) {
        return api.beginTransaction(type, loginContext, clientInfo, timeout, unit);
    }

    @Override
    public boolean isAvailable(long timeout) {
        return api.isAvailable(timeout);
    }

    @Override
    public Transaction beginTx() {
        return api.beginTx();
    }

    @Override
    public Transaction beginTx(long timeout, TimeUnit unit) {
        return api.beginTx(timeout, unit);
    }

    @Override
    public void executeTransactionally(String query) throws QueryExecutionException {
        api.executeTransactionally(query);
    }

    @Override
    public void executeTransactionally(String query, Map<String, Object> parameters) throws QueryExecutionException {
        api.executeTransactionally(query, parameters);
    }

    @Override
    public <T> T executeTransactionally(
        String query,
        Map<String, Object> parameters,
        ResultTransformer<T> resultTransformer
    ) throws QueryExecutionException {
        return api.executeTransactionally(query, parameters, resultTransformer);
    }

    @Override
    public <T> T executeTransactionally(
        String query,
        Map<String, Object> parameters,
        ResultTransformer<T> resultTransformer,
        Duration timeout
    ) throws QueryExecutionException {
        return api.executeTransactionally(query, parameters, resultTransformer, timeout);
    }

    @Override
    public String databaseName() {
        return api.databaseName();
    }

    public GraphDatabaseAPI api() {
        return api;
    }
}
