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
package org.neo4j.graphalgo.core.loading;

import org.neo4j.graphalgo.api.GraphLoaderContext;
import org.neo4j.graphalgo.config.GraphCreateFromCypherConfig;
import org.neo4j.graphalgo.utils.StringJoining;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.security.AuthorizationViolationException;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.neo4j.gds.compat.GraphDatabaseApiProxy.runQueryWithoutClosingTheResult;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

abstract class CypherRecordLoader<R> {

    enum QueryType {
        NODE, RELATIONSHIP;

        String toLowerCase() {
            return name().toLowerCase(Locale.ENGLISH);
        }
    }

    static final long NO_COUNT = -1L;

    final GraphCreateFromCypherConfig cypherConfig;
    final GraphLoaderContext loadingContext;

    private final long recordCount;
    private final String loadQuery;

    CypherRecordLoader(
        String loadQuery,
        long recordCount,
        GraphCreateFromCypherConfig cypherConfig,
        GraphLoaderContext loadingContext
    ) {
        this.loadQuery = loadQuery;
        this.recordCount = recordCount;
        this.cypherConfig = cypherConfig;
        this.loadingContext = loadingContext;
    }

    final R load(Transaction transaction) {
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
        Transaction tx,
        int bufferSize
    );

    abstract void updateCounts(BatchLoadResult result);

    abstract R result();

    abstract Set<String> getMandatoryColumns();

    abstract Set<String> getReservedColumns();

    Collection<String> getPropertyColumns(Result queryResult) {
        Predicate<String> contains = getReservedColumns()::contains;
        return queryResult
            .columns()
            .stream()
            .filter(contains.negate())
            .collect(Collectors.toList());
    }

    Result runLoadingQuery(Transaction tx) {
        Result result = runQueryWithoutClosingTheResult(tx, loadQuery, cypherConfig.parameters());
        validateMandatoryColumns(List.copyOf(result.columns()));
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
}
