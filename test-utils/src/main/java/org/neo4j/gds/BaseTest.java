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
package org.neo4j.gds;


import org.assertj.core.api.Assertions;
import org.intellij.lang.annotations.Language;
import org.neo4j.gds.core.Settings;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.Neo4jGraphExtension;
import org.neo4j.gds.extension.NodeFunction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.ImpermanentDbmsExtension;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

@ImpermanentDbmsExtension(configurationCallback = "configuration")
@Neo4jGraphExtension
public abstract class BaseTest {

    protected static final String DEFAULT_GRAPH_NAME = "graph";

    @Inject
    public GraphDatabaseService db;

    @Inject
    public NodeFunction nodeFunction;

    @Inject
    public IdFunction idFunction;

    @ExtensionCallback
    protected void configuration(TestDatabaseManagementServiceBuilder builder) {
        builder.noOpSystemGraphInitializer();
        builder.setConfig(Settings.procedureUnrestricted(), singletonList("gds.*"));
        // A change in 4.3.0-drop02.0 is enabling the feature to track cursor.close() events by default
        // for test databases. We would like to additionally enable the feature to trace cursors,
        // so that when we leak cursors, we can get a stacktrace of who was creating them.
        // This is a no-op in any version before 4.3.0-drop02.0, where this behavior is governed by feature toggles
        // but those are not enabled by default, test scope or otherwise.
        builder.setConfigRaw(Map.of("unsupported.dbms.debug.track_cursor_close", "true"));
        builder.setConfigRaw(Map.of("unsupported.dbms.debug.trace_cursors", "true"));
    }

    protected long clearDb() {
        var deletedNodes = new AtomicLong();
        runQueryWithRowConsumer("MATCH (n) DETACH DELETE n RETURN count(n)",
            row -> deletedNodes.set(row.getNumber("count(n)").longValue()));
        return deletedNodes.get();
    }

    protected List<Node> allNodes() {
        var sourceNodes = new ArrayList<Node>();
        runQueryWithRowConsumer("MATCH (n) RETURN n", row -> sourceNodes.add(row.getNode("n")));
        return sourceNodes;
    }

    protected List<Node> allNodesWithLabel(String label) {
        var sourceNodes = new ArrayList<Node>();
        runQueryWithRowConsumer(formatWithLocale("MATCH (n:%s) RETURN n", label), row -> sourceNodes.add(row.getNode("n")));
        return sourceNodes;
    }

    protected void runQueryWithRowConsumer(
        @Language("Cypher") String query,
        Consumer<Result.ResultRow> check
    ) {
        QueryRunner.runQueryWithRowConsumer(db, query, check);
    }

    protected void runQueryWithRowConsumer(
        @Language("Cypher") String query,
        BiConsumer<Transaction, Result.ResultRow> check
    ) {
        QueryRunner.runQueryWithRowConsumer(db, query, emptyMap(), check);
    }

    protected void runQueryWithRowConsumer(
        @Language("Cypher") String query,
        Map<String, Object> params,
        Consumer<Result.ResultRow> check
    ) {
        QueryRunner.runQueryWithRowConsumer(db, query, params, discardTx(check));
    }

    protected void runQueryWithRowConsumer(
        GraphDatabaseService localDb,
        @Language("Cypher") String query,
        Map<String, Object> params,
        Consumer<Result.ResultRow> check
    ) {
        QueryRunner.runQueryWithRowConsumer(localDb, query, params, discardTx(check));
    }

    protected void runQueryWithRowConsumer(
        GraphDatabaseService localDb,
        @Language("Cypher") String query,
        Consumer<Result.ResultRow> check
    ) {
        QueryRunner.runQueryWithRowConsumer(localDb, query, emptyMap(), discardTx(check));
    }

    protected void runQueryWithRowConsumer(
        String username,
        @Language("Cypher") String query,
        Consumer<Result.ResultRow> check
    ) {
        QueryRunner.runQueryWithRowConsumer(db, username, query, emptyMap(), discardTx(check));
    }

    protected <T> T runQuery(
        String username,
        @Language("Cypher") String query,
        Function<Result, T> resultFunction
    ) {
        return QueryRunner.runQuery(db, username, query, emptyMap(), resultFunction);
    }

    protected void runQuery(
        String username,
        @Language("Cypher") String query,
        Map<String, Object> params
    ) {
        QueryRunner.runQuery(db, username, query, params);
    }

    protected void runQuery(
        String username,
        @Language("Cypher") String query
    ) {
        runQuery(username, query, Map.of());
    }

    protected void runQuery(
        GraphDatabaseService db,
        @Language("Cypher") String query,
        Map<String, Object> params
    ) {
        QueryRunner.runQuery(db, query, params);
    }

    protected void runQuery(@Language("Cypher") String query) {
        QueryRunner.runQuery(db, query);
    }

    protected void runQuery(
        @Language("Cypher") String query,
        Map<String, Object> params
    ) {
        QueryRunner.runQuery(db, query, params);
    }

    protected <T> T runQuery(
        @Language("Cypher") String query,
        Function<Result, T> resultFunction
    ) {
        return QueryRunner.runQuery(db, query, emptyMap(), resultFunction);
    }

    protected <T> T runQuery(
        @Language("Cypher") String query,
        Map<String, Object> params,
        Function<Result, T> resultFunction
    ) {
        return QueryRunner.runQuery(db, query, params, resultFunction);
    }

    protected <T> T runQuery(
        GraphDatabaseService db,
        @Language("Cypher") String query,
        Map<String, Object> params,
        Function<Result, T> resultFunction
    ) {
        return QueryRunner.runQuery(db, query, params, resultFunction);
    }

    protected void runQueryWithResultConsumer(
        @Language("Cypher") String query,
        Map<String, Object> params,
        Consumer<Result> check
    ) {
        QueryRunner.runQueryWithResultConsumer(
            db,
            query,
            params,
            check
        );
    }

    protected void runQueryWithResultConsumer(
        @Language("Cypher") String query,
        Consumer<Result> check
    ) {
        QueryRunner.runQueryWithResultConsumer(
            db,
            query,
            emptyMap(),
            check
        );
    }

    protected void runQueryWithResultConsumer(
        String operatorName,
        @Language("Cypher") String query,
        Consumer<Result> check
    ) {
        QueryRunner.runQueryWithResultConsumer(
            db,
            operatorName,
            query,
            emptyMap(),
            check
        );
    }

    private static BiConsumer<Transaction, Result.ResultRow> discardTx(Consumer<Result.ResultRow> check) {
        return (tx, row) -> check.accept(row);
    }

    protected void assertCypherResult(@Language("Cypher") String query, List<Map<String, Object>> expected) {
        assertCypherResult(query, emptyMap(), expected);
    }

    protected void assertCypherResult(
        @Language("Cypher") String query,
        Map<String, Object> queryParameters,
        List<Map<String, Object>> expected
    ) {
        TestSupport.assertCypherResult(db, query, queryParameters, expected);
    }

    static {
        // configure AssertJ to consider 'name()' as a method for the property 'name' instead of just 'getName()'
        Assertions.setExtractBareNamePropertyMethods(true);
    }
}
