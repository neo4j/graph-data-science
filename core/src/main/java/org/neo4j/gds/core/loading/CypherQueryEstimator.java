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

import org.immutables.value.Value;
import org.neo4j.gds.PropertyMapping;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.transaction.TransactionContext;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;
import static org.neo4j.internal.kernel.api.security.AccessMode.Static.READ;
import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_PROPERTY_KEY;

public class CypherQueryEstimator {

    private final TransactionContext context;

    CypherQueryEstimator(TransactionContext context) {this.context = context;}


    EstimationResult getNodeEstimation(String nodeQuery) {
        return runEstimationQuery(nodeQuery, NodeSubscriber.RESERVED_COLUMNS);
    }

    EstimationResult getRelationshipEstimation(String relationshipQuery) {
        return runEstimationQuery(
            relationshipQuery,
            RelationshipSubscriber.RESERVED_COLUMNS
        );
    }

    private EstimationResult runEstimationQuery(String query, Collection<String> reservedColumns) {
        return context.withRestrictedAccess(READ).apply((tx, ktx) -> {
            var explainQuery = formatWithLocale("EXPLAIN %s", query);
            try (var result = tx.execute(explainQuery)) {
                var estimatedRows = (Number) result.getExecutionPlanDescription().getArguments().get("EstimatedRows");

                var propertyColumns = new ArrayList<>(result.columns());
                propertyColumns.removeAll(reservedColumns);

                return ImmutableEstimationResult.of(estimatedRows.longValue(), propertyColumns.size());
            }
        });
    }

    @ValueClass
    public
    interface EstimationResult {
        long estimatedRows();

        long propertyCount();

        @Value.Derived
        default Map<String, Integer> propertyTokens() {
            return LongStream
                .range(0, propertyCount())
                .boxed()
                .collect(Collectors.toMap(
                    Object::toString,
                    property -> NO_SUCH_PROPERTY_KEY
                ));
        }

        @Value.Derived
        default Collection<PropertyMapping> propertyMappings() {
            return LongStream
                .range(0, propertyCount())
                .mapToObj(property -> PropertyMapping.of(Long.toString(property), DefaultValue.DEFAULT))
                .collect(Collectors.toList());
        }

    }
}
