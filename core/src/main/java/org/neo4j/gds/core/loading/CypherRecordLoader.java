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
package org.neo4j.gds.core.loading;

import org.neo4j.gds.api.GraphLoaderContext;
import org.neo4j.gds.compat.GraphDatabaseApiProxy;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.config.GraphProjectFromCypherConfig;
import org.neo4j.gds.utils.StringJoining;
import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.query.QueryExecution;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.impl.query.QueryExecutionKernelException;
import org.neo4j.kernel.impl.query.QuerySubscriber;
import org.neo4j.kernel.impl.query.TransactionalContextFactory;
import org.neo4j.kernel.impl.util.ValueUtils;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

abstract class CypherRecordLoader<R> {


    enum QueryType {
        NODE, RELATIONSHIP;

        String toLowerCase() {
            return name().toLowerCase(Locale.ENGLISH);
        }
    }

    static final long NO_COUNT = -1L;

    final GraphProjectFromCypherConfig cypherConfig;
    final GraphLoaderContext loadingContext;

    private final long recordCount;
    private final String loadQuery;
    private final QueryExecutionEngine executionEngine;
    private final TransactionalContextFactory contextFactory;

    CypherRecordLoader(
        String loadQuery,
        long recordCount,
        GraphProjectFromCypherConfig cypherConfig,
        GraphLoaderContext loadingContext
    ) {
        this.loadQuery = loadQuery;
        this.recordCount = recordCount;
        this.cypherConfig = cypherConfig;
        this.loadingContext = loadingContext;
        this.executionEngine =  GraphDatabaseApiProxy.executionEngine(loadingContext.graphDatabaseService());
        this.contextFactory = GraphDatabaseApiProxy.contextFactory(loadingContext.graphDatabaseService());
    }

    final R load(InternalTransaction transaction) {
        try {
            int bufferSize = (int) Math.min(recordCount, RecordsBatchBuffer.DEFAULT_BUFFER_SIZE);
            BatchLoadResult result = loadSingleBatch(transaction, bufferSize);
            updateCounts(result);
            return result();
        } catch (AuthorizationViolationException ex) {
            throw new IllegalArgumentException(formatWithLocale("Query must be read only. Query: [%s]", loadQuery));
        }
    }

    abstract QueryType queryType();

    abstract BatchLoadResult loadSingleBatch(
        InternalTransaction tx,
        int bufferSize
    );

    abstract void updateCounts(BatchLoadResult result);

    abstract R result();

    abstract Set<String> getMandatoryColumns();

    abstract Set<String> getReservedColumns();

    Collection<String> getPropertyColumns(QueryExecution queryResult) {
        Predicate<String> contains = getReservedColumns()::contains;
        return Arrays.stream(queryResult.fieldNames())
            .filter(contains.negate())
            .collect(Collectors.toList());
    }

    boolean columnsContains(QueryExecution queryResult, String name) {
        for (String fieldName : queryResult.fieldNames()) {
            if (fieldName.equals(name)) {
                return true;
            }
        }
        return false;
    }

    QueryExecution runLoadingQuery(InternalTransaction tx, QuerySubscriber subscriber) {
        var result = runQueryWithoutClosingTheResult(
            tx,
            loadQuery,
            cypherConfig.parameters(),
            contextFactory,
            executionEngine,
            subscriber
        );
        validateMandatoryColumns(Arrays.asList(result.fieldNames()));
        return result;
    }

    private void validateMandatoryColumns(Collection<String> allColumns) {
        var missingColumns = new HashSet<>(getMandatoryColumns());
        missingColumns.removeAll(allColumns);
        if (!missingColumns.isEmpty()) {
            throw new IllegalArgumentException(formatWithLocale(
                "Invalid %s query, required column(s) not found: '%s' - did you specify %s?",
                queryType().toLowerCase(),
                StringJoining.join(missingColumns, "', '"),
                StringJoining.joinVerbose(
                    missingColumns.stream()
                        .map(column -> "'AS " + column + "'")
                        .collect(Collectors.toList()))
            ));
        }
    }

    private static QueryExecution runQueryWithoutClosingTheResult(
        InternalTransaction tx,
        String query,
        Map<String, Object> params,
        TransactionalContextFactory contextFactory,
        QueryExecutionEngine executionEngine,
        QuerySubscriber subscriber
    ) {
        var convertedParams = ValueUtils.asMapValue(params);
        var context = Neo4jProxy.newQueryContext(contextFactory, tx, query, convertedParams);
        try {
            return executionEngine.executeQuery(query, convertedParams, context, false, subscriber);
        } catch (QueryExecutionKernelException e) {
            throw e.asUserException();
        }
    }

}
