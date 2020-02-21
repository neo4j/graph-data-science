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
package org.neo4j.graphalgo;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.KernelEventHandler;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.graphdb.security.URLAccessValidationError;
import org.neo4j.graphdb.traversal.BidirectionalTraversalDescription;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.StoreId;

import java.net.URL;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

public final class TestDatabaseApi implements GraphDatabaseAPI {
    private final GraphDatabaseAPI api;

    TestDatabaseApi(GraphDatabaseAPI api) {
        this.api = api;
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

    // delegate methods

    @Override
    public void shutdown() {
        api.shutdown();
    }

    @Override
    public DependencyResolver getDependencyResolver() {
        return api.getDependencyResolver();
    }

    @Override
    public StoreId storeId() {
        return api.storeId();
    }

    @Override
    public URL validateURLAccess(URL url) throws URLAccessValidationError {
        return api.validateURLAccess(url);
    }

    @Override
    public DatabaseLayout databaseLayout() {
        return api.databaseLayout();
    }

    @Override
    public InternalTransaction beginTransaction(
        org.neo4j.internal.kernel.api.Transaction.Type type,
        LoginContext loginContext
    ) {
        return api.beginTransaction(type, loginContext);
    }

    @Override
    public InternalTransaction beginTransaction(
        org.neo4j.internal.kernel.api.Transaction.Type type,
        LoginContext loginContext,
        long timeout,
        TimeUnit unit
    ) {
        return api.beginTransaction(type, loginContext, timeout, unit);
    }

    @Override
    public Node createNode() {
        return api.createNode();
    }

    @Override
    @Deprecated
    public Long createNodeId() {
        return api.createNodeId();
    }

    @Override
    public Node createNode(Label... labels) {
        return api.createNode(labels);
    }

    @Override
    public Node getNodeById(long id) {
        return api.getNodeById(id);
    }

    @Override
    public Relationship getRelationshipById(long id) {
        return api.getRelationshipById(id);
    }

    @Override
    public ResourceIterable<Node> getAllNodes() {
        return api.getAllNodes();
    }

    @Override
    public ResourceIterable<Relationship> getAllRelationships() {
        return api.getAllRelationships();
    }

    @Override
    public ResourceIterator<Node> findNodes(Label label, String key, Object value) {
        return api.findNodes(label, key, value);
    }

    @Override
    public Node findNode(Label label, String key, Object value) {
        return api.findNode(label, key, value);
    }

    @Override
    public ResourceIterator<Node> findNodes(Label label) {
        return api.findNodes(label);
    }

    @Override
    public ResourceIterable<Label> getAllLabelsInUse() {
        return api.getAllLabelsInUse();
    }

    @Override
    public ResourceIterable<RelationshipType> getAllRelationshipTypesInUse() {
        return api.getAllRelationshipTypesInUse();
    }

    @Override
    public ResourceIterable<Label> getAllLabels() {
        return api.getAllLabels();
    }

    @Override
    public ResourceIterable<RelationshipType> getAllRelationshipTypes() {
        return api.getAllRelationshipTypes();
    }

    @Override
    public ResourceIterable<String> getAllPropertyKeys() {
        return api.getAllPropertyKeys();
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
    public Result execute(String query, long timeout, TimeUnit unit) throws QueryExecutionException {
        return api.execute(query, timeout, unit);
    }

    @Override
    public Result execute(String query, Map<String, Object> parameters, long timeout, TimeUnit unit) throws
        QueryExecutionException {
        return api.execute(query, parameters, timeout, unit);
    }

    @Override
    public <T> TransactionEventHandler<T> registerTransactionEventHandler(TransactionEventHandler<T> handler) {
        return api.registerTransactionEventHandler(handler);
    }

    @Override
    public <T> TransactionEventHandler<T> unregisterTransactionEventHandler(TransactionEventHandler<T> handler) {
        return api.unregisterTransactionEventHandler(handler);
    }

    @Override
    public KernelEventHandler registerKernelEventHandler(KernelEventHandler handler) {
        return api.registerKernelEventHandler(handler);
    }

    @Override
    public KernelEventHandler unregisterKernelEventHandler(KernelEventHandler handler) {
        return api.unregisterKernelEventHandler(handler);
    }

    @Override
    public Schema schema() {
        return api.schema();
    }

    @Override
    @Deprecated
    public IndexManager index() {
        return api.index();
    }

    @Override
    public TraversalDescription traversalDescription() {
        return api.traversalDescription();
    }

    @Override
    public BidirectionalTraversalDescription bidirectionalTraversalDescription() {
        return api.bidirectionalTraversalDescription();
    }

    @Override
    public Result execute(String query) throws QueryExecutionException {
        return api.execute(query);
    }

    @Override
    public Result execute(String query, Map<String, Object> parameters) throws QueryExecutionException {
        return api.execute(query, parameters);
    }

    public GraphDatabaseAPI api() {
        return api;
    }
}
