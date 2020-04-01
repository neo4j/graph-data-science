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
package org.neo4j.graphalgo.core.loading;

import org.apache.commons.compress.utils.Lists;
import org.neo4j.graphalgo.ResolvedPropertyMapping;
import org.neo4j.graphalgo.ResolvedPropertyMappings;
import org.neo4j.graphalgo.api.GraphSetup;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.neo4j.graphalgo.compat.GraphDatabaseApiProxy.runQueryWithoutClosingTheResult;

abstract class CypherRecordLoader<R> {

    enum QueryType {
        NODE, RELATIONSHIP;

        String capitalize() {
            return name().substring(0, 1) + name().substring(1).toLowerCase();
        }

        String toLowerCase() {
            return name().toLowerCase();
        }
    }

    static final long NO_COUNT = -1L;

    final GraphCreateConfig config;
    final GraphSetup setup;

    protected final GraphDatabaseAPI api;

    private final long recordCount;
    private final String loadQuery;

    CypherRecordLoader(String loadQuery, long recordCount, GraphDatabaseAPI api, GraphCreateConfig config, GraphSetup setup) {
        this.loadQuery = loadQuery;
        this.recordCount = recordCount;
        this.api = api;
        this.config = config;
        this.setup = setup;
    }

    final R load(CypherFactory.Ktx ktx) {
        try {
            int bufferSize = (int) Math.min(recordCount, RecordsBatchBuffer.DEFAULT_BUFFER_SIZE);
            BatchLoadResult result = ktx.run(tx -> loadSingleBatch(tx, bufferSize));
            updateCounts(result);
            return result();
        } catch (AuthorizationViolationException ex) {
            throw new IllegalArgumentException(String.format("Query must be read only. Query: [%s]", loadQuery));
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

    List<String> getPropertyColumns(Result queryResult) {
        Predicate<String> contains = getReservedColumns()::contains;
        return queryResult
            .columns()
            .stream()
            .filter(contains.negate())
            .collect(Collectors.toList());
    }

    Result runLoadingQuery(Transaction tx) {
        Result result = runQueryWithoutClosingTheResult(api, tx, loadQuery, setup.parameters());
        validateMandatoryColumns(Lists.newArrayList(result.columns().iterator()));
        return result;
    }

    private void validateMandatoryColumns(List<String> allColumns) {
        Set<String> missingColumns = new HashSet<>(getMandatoryColumns());
        missingColumns.removeAll(allColumns);
        if (!missingColumns.isEmpty()) {
            throw new IllegalArgumentException(String.format(
                "Invalid %s query, required column(s) not found: '%s'",
                queryType().toLowerCase(),
                String.join("', '", missingColumns)
            ));
        }
    }

    void validatePropertyColumns(
        Collection<String> propertyColumns,
        ResolvedPropertyMappings resolvedPropertyMappings
    ) {
        List<String> invalidNodeProperties = resolvedPropertyMappings
            .mappings()
            .stream()
            .map(ResolvedPropertyMapping::neoPropertyKey)
            .filter(k -> !propertyColumns.contains(k))
            .collect(Collectors.toList());

        if (!invalidNodeProperties.isEmpty()) {
            throw new IllegalArgumentException(String.format(
                "%s properties not found: '%s'. Available properties from the %s query are: '%s'",
                queryType().capitalize(),
                String.join("', '", invalidNodeProperties),
                queryType().toLowerCase(),
                String.join("', '", propertyColumns)
            ));
        }
    }

}
