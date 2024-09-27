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
package org.neo4j.gds.projection;

import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.transaction.TransactionContext;
import org.neo4j.graphdb.ExecutionPlanDescription;

import java.util.Optional;

final class QueryRowEstimationUtil {

    private QueryRowEstimationUtil() {}

    /**
     * This method traverses the execution plan of the given cypher query to find the _child_ node
     * of the cypher aggregation node. This child node contains the estimated number of rows that
     * will be returned by the query.
     */
    static int estimatedRows(TransactionContext transactionContext, String query) {
        //noinspection resource
        try (var tx = transactionContext.fork().transaction()) {
            var executionPlan = tx.execute(explainQuery(query)).getExecutionPlanDescription();
            tx.commit();
            var aggregationChild = findChildOfRemoteAggregation(executionPlan);
            return aggregationChild
                .map(child -> ((Number) child.getArguments().get("EstimatedRows")).intValue())
                .orElse(Task.UNKNOWN_VOLUME);
        } catch (Exception e) {
            return Task.UNKNOWN_VOLUME;
        }
    }

    /**
     * Traverses the execution plan to find an `EagerAggregation` operator with the remote projection aggregation.
     */
    private static Optional<ExecutionPlanDescription> findChildOfRemoteAggregation(ExecutionPlanDescription plan) {
        if (plan.getName().equals("EagerAggregation")
            && ((String) plan.getArguments().get("Details")).contains(CypherAggregation.FUNCTION_NAME.toString())) {
            var children = plan.getChildren();
            if (children.size() == 1) {
                return Optional.of(children.get(0));
            }
            return Optional.empty();
        }

        if (plan.getChildren().isEmpty()) {
            return Optional.empty();
        }

        return plan.getChildren().stream()
            .map(QueryRowEstimationUtil::findChildOfRemoteAggregation)
            .flatMap(Optional::stream)
            .findFirst();
    }

    private static String explainQuery(String query) {
        return "EXPLAIN " + query;
    }
}
