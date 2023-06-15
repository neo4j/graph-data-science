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
import org.neo4j.gds.config.GraphProjectFromCypherConfig;
import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;

import java.util.Locale;
import java.util.Set;

class CountingCypherRecordLoader extends CypherRecordLoader<BatchLoadResult> {

    private final QueryType queryType;

    private long total;

    CountingCypherRecordLoader(
        String cypherQuery,
        QueryType queryType,
        GraphProjectFromCypherConfig cypherConfig,
        GraphLoaderContext loadingContext
    ) {
        super(cypherQuery, NO_COUNT, cypherConfig, loadingContext);
        this.queryType = queryType;
    }

    @Override
    BatchLoadResult loadSingleBatch(InternalTransaction tx, int bufferSize) {
        var subscriber = new ResultCountingSubscriber();
        CypherLoadingUtils.consume(runLoadingQuery(tx, subscriber));

        subscriber.error().ifPresent(e -> {
            if (e instanceof AuthorizationViolationException) {
                throw new IllegalArgumentException(String.format(
                    Locale.US,
                    "Query must be read only. Query: [%s]",
                    this.loadQuery
                ));
            }

            throw new RuntimeException(e);
        });

        return new BatchLoadResult(subscriber.rows(), -1L);
    }

    @Override
    void updateCounts(BatchLoadResult result) {
        total += result.rows();
    }

    @Override
    BatchLoadResult result() {
        return new BatchLoadResult(total, -1L);
    }

    @Override
    Set<String> getMandatoryColumns() {
        return queryType == QueryType.NODE
            ? NodeSubscriber.REQUIRED_COLUMNS
            : RelationshipSubscriber.REQUIRED_COLUMNS;
    }

    @Override
    Set<String> getReservedColumns() {
        return queryType == QueryType.NODE
            ? NodeSubscriber.RESERVED_COLUMNS
            : RelationshipSubscriber.RESERVED_COLUMNS;
    }

    @Override
    QueryType queryType() {
        return queryType;
    }
}
