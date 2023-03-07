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

import org.apache.commons.lang3.mutable.MutableLong;
import org.intellij.lang.annotations.Language;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;

import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.util.Collections.emptyMap;
import static org.neo4j.gds.compat.GraphDatabaseApiProxy.applyInTransaction;
import static org.neo4j.gds.compat.GraphDatabaseApiProxy.runInTransaction;
import static org.neo4j.gds.compat.GraphDatabaseApiProxy.runQueryWithoutClosingTheResult;
import static org.neo4j.internal.kernel.api.security.AccessMode.Static.READ;

public final class QueryRunner {

    private static final Result.ResultVisitor<RuntimeException> CONSUME_ROWS = ignore -> true;

    private QueryRunner() {}

    public static long runQueryWithRowConsumer(
        GraphDatabaseService db,
        String username,
        @Language("Cypher") String query,
        Map<String, Object> params,
        BiConsumer<Transaction, Result.ResultRow> rowConsumer
    ) {
        var rowCounter = new MutableLong();
        runInTransaction(db, tx -> {
            try (KernelTransaction.Revertable ignored = withUsername(tx, username, db.databaseName());
                 Result result = runQueryWithoutClosingTheResult(tx, query, params)) {
                result.accept(row -> {
                    rowConsumer.accept(tx, row);
                    rowCounter.increment();

                    return true;
                });
            }
        });

        return rowCounter.longValue();
    }

    public static long runQueryWithRowConsumer(
        GraphDatabaseService db,
        @Language("Cypher") String query,
        Map<String, Object> params,
        BiConsumer<Transaction, Result.ResultRow> rowConsumer
    ) {
        var rowCounter = new MutableLong();
        runInTransaction(db, tx -> {
            try (Result result = runQueryWithoutClosingTheResult(tx, query, params)) {
                result.accept(row -> {
                    rowConsumer.accept(tx, row);
                    rowCounter.increment();

                    return true;
                });
            }
        });

        return rowCounter.longValue();
    }

    public static long runQueryWithRowConsumer(
        GraphDatabaseService db,
        @Language("Cypher") String query,
        Consumer<Result.ResultRow> rowConsumer
    ) {
        var rowCounter = new MutableLong();
        runInTransaction(db, tx -> {
            try (Result result = runQueryWithoutClosingTheResult(tx, query, emptyMap())) {
                result.accept(row -> {
                    rowConsumer.accept(row);
                    rowCounter.increment();

                    return true;
                });
            }
        });

        return rowCounter.longValue();
    }

    public static void runQuery(GraphDatabaseService db, @Language("Cypher") String query) {
        runQuery(db, query, emptyMap());
    }

    public static void runQuery(GraphDatabaseService db, @Language("Cypher") String query, Map<String, Object> params) {
        runInTransaction(db, tx -> {
            try (Result result = runQueryWithoutClosingTheResult(tx, query, params)) {
                result.accept(CONSUME_ROWS);
            }
        });
    }

    public static <T> T runQuery(
        GraphDatabaseService db,
        String username,
        @Language("Cypher") String query,
        Map<String, Object> params,
        Function<Result, T> resultFunction
    ) {
        return applyInTransaction(db, tx -> {
            try (
                KernelTransaction.Revertable ignored = withUsername(tx, username, db.databaseName());
                Result result = runQueryWithoutClosingTheResult(tx, query, params)
            ) {
                return resultFunction.apply(result);
            }
        });
    }

    public static void runQuery(
        GraphDatabaseService db,
        String username,
        @Language("Cypher") String query,
        Map<String, Object> params
    ) {
        runInTransaction(db, tx -> {
            try (KernelTransaction.Revertable ignored = withUsername(tx, username, db.databaseName());
                 Result result = runQueryWithoutClosingTheResult(tx, query, params)) {
                result.accept(CONSUME_ROWS);
            }
        });
    }

    public static <T> T runQuery(
        GraphDatabaseService db,
        @Language("Cypher") String query,
        Function<Result, T> resultFunction
    ) {
        return runQuery(db, query, emptyMap(), resultFunction);
    }

    public static <T> T runQuery(
        GraphDatabaseService db,
        @Language("Cypher") String query,
        Map<String, Object> params,
        Function<Result, T> resultFunction
    ) {
        return applyInTransaction(db, tx -> {
            try (var result = runQueryWithoutClosingTheResult(tx, query, params)) {
                return resultFunction.apply(result);
            }
        });
    }

    public static void runQueryWithResultConsumer(
        GraphDatabaseService db,
        @Language("Cypher") String query,
        Map<String, Object> params,
        Consumer<Result> resultConsumer
    ) {
        runInTransaction(db, tx -> {
            try (var result = runQueryWithoutClosingTheResult(tx, query, params)) {
                resultConsumer.accept(result);
            }
        });
    }

    public static void runQueryWithResultConsumer(
        GraphDatabaseService db,
        String username,
        @Language("Cypher") String query,
        Map<String, Object> params,
        Consumer<Result> resultConsumer
    ) {
        runInTransaction(db, tx -> {
            try (KernelTransaction.Revertable ignored = withUsername(tx, username, db.databaseName())) {
                try (var result = runQueryWithoutClosingTheResult(tx, query, params)) {
                    resultConsumer.accept(result);
                }
            }
        });
    }

    private static KernelTransaction.Revertable withUsername(Transaction tx, String username, String databaseName) {
        InternalTransaction topLevelTransaction = (InternalTransaction) tx;
        AuthSubject subject = topLevelTransaction.securityContext().subject();
        var securityContext = Neo4jProxy.securityContext(username, subject, READ, databaseName);
        return topLevelTransaction.overrideWith(securityContext);
    }
}
