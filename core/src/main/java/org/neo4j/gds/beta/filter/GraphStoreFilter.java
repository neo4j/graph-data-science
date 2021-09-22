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
package org.neo4j.gds.beta.filter;

import org.jetbrains.annotations.NotNull;
import org.neo4j.gds.ElementProjection;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.beta.filter.expression.Expression;
import org.neo4j.gds.beta.filter.expression.ExpressionParser;
import org.neo4j.gds.beta.filter.expression.SemanticErrors;
import org.neo4j.gds.beta.filter.expression.ValidationContext;
import org.neo4j.gds.config.GraphCreateFromGraphConfig;
import org.neo4j.gds.core.loading.CSRGraphStore;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.opencypher.v9_0.parser.javacc.ParseException;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;

public final class GraphStoreFilter {

    public static Task progressTask(GraphStore graphStore) {
        var nodePropertyCount = graphStore.nodePropertyKeys().values().stream().mapToInt(Set::size).sum();
        return progressTask(
            graphStore.nodeCount(),
            nodePropertyCount,
            graphStore.relationshipTypes().size()
        );
    }

    public static Task progressTask(long nodeCount, long nodePropertyCount, int relationshipTypes) {
        var nodesTask = Tasks.leaf("Nodes", nodeCount);
        var nodePropertiesTask = Tasks.iterativeOpen(
            "Node properties",
            () -> List.of(
                Tasks.leaf("Label", nodeCount * nodePropertyCount)
            )
        );

        var relationshipsTask = Tasks.iterativeFixed(
            "Relationships",
            () -> List.of(
                Tasks.leaf("Relationship type")
            ),
            relationshipTypes
        );

        return Tasks.task(
            "GraphStore Filter",
            nodesTask,
            nodePropertiesTask,
            relationshipsTask
        );
    }

    @NotNull
    public static GraphStore filter(
        GraphStore graphStore,
        GraphCreateFromGraphConfig config,
        ExecutorService executorService,
        AllocationTracker allocationTracker,
        ProgressTracker progressTracker
    ) throws ParseException, SemanticErrors {
        var expressions = parseAndValidate(graphStore, config.nodeFilter(), config.relationshipFilter());

        var inputNodes = graphStore.nodes();

        progressTracker.beginSubTask();
        try {
            var filteredNodes = NodesFilter.filterNodes(
                graphStore,
                expressions.nodeExpression(),
                config.concurrency(),
                executorService,
                progressTracker,
                allocationTracker
            );

            var filteredRelationships = RelationshipsFilter.filterRelationships(
                graphStore,
                expressions.relationshipExpression(),
                inputNodes,
                filteredNodes.nodeMapping(),
                config.concurrency(),
                executorService,
                progressTracker,
                allocationTracker
            );

            return CSRGraphStore.of(
                graphStore.databaseId(),
                filteredNodes.nodeMapping(),
                filteredNodes.propertyStores(),
                filteredRelationships.topology(),
                filteredRelationships.propertyStores(),
                config.concurrency(),
                allocationTracker
            );
        } finally {
            progressTracker.endSubTask();
        }
    }

    @ValueClass
    interface Expressions {
        Expression nodeExpression();

        Expression relationshipExpression();
    }

    private static Expressions parseAndValidate(
        GraphStore graphStore,
        String nodeFilter,
        String relationshipFilter
    ) throws ParseException, SemanticErrors {
        var nodeExpression = ExpressionParser.parse(replaceStarWithTrue(nodeFilter));
        var relationshipExpression = ExpressionParser.parse(replaceStarWithTrue(relationshipFilter));

        nodeExpression.validate(ValidationContext.forNodes(graphStore)).validate();
        relationshipExpression.validate(ValidationContext.forRelationships(graphStore)).validate();

        return ImmutableExpressions.of(nodeExpression, relationshipExpression);
    }

    private static String replaceStarWithTrue(String filter) {
        return filter.equals(ElementProjection.PROJECT_ALL) ? "true" : filter;
    }

    private GraphStoreFilter() {}
}
