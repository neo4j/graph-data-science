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
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.schema.Direction;
import org.neo4j.gds.api.schema.GraphSchema;
import org.neo4j.gds.api.schema.RelationshipSchema;
import org.neo4j.gds.beta.filter.expression.Expression;
import org.neo4j.gds.beta.filter.expression.ExpressionParser;
import org.neo4j.gds.beta.filter.expression.SemanticErrors;
import org.neo4j.gds.beta.filter.expression.ValidationContext;
import org.neo4j.gds.config.GraphProjectFromGraphConfig;
import org.neo4j.gds.core.loading.GraphStoreBuilder;
import org.neo4j.gds.core.loading.RelationshipImportResult;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.opencypher.v9_0.parser.javacc.ParseException;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

public final class GraphStoreFilter {

    public static Task progressTask(GraphStore graphStore) {
        var nodePropertyCount = graphStore.nodePropertyKeys().size();
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
        GraphProjectFromGraphConfig config,
        ExecutorService executorService,
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
                config.parameters(),
                executorService,
                progressTracker
            );

            var filteredRelationships = RelationshipsFilter.filterRelationships(
                graphStore,
                expressions.relationshipExpression(),
                inputNodes,
                filteredNodes.idMap(),
                config.concurrency(),
                config.parameters(),
                executorService,
                progressTracker
            );

            var filteredSchema = filterSchema(graphStore.schema(), filteredNodes, filteredRelationships);

            return new GraphStoreBuilder()
                .databaseId(graphStore.databaseId())
                .capabilities(graphStore.capabilities())
                .schema(filteredSchema)
                .nodes(filteredNodes.idMap())
                .nodePropertyStore(filteredNodes.propertyStores())
                .relationshipImportResult(RelationshipImportResult.of(
                    filteredRelationships.topology(),
                    filteredRelationships.propertyStores(),
                    filteredSchema.relationshipSchema().directions()
                ))
                .concurrency(config.concurrency())
                .build();
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
    ) throws IllegalArgumentException {
        Expression nodeExpression;
        try {
            var validationContext = ValidationContext.forNodes(graphStore);
            nodeExpression = ExpressionParser.parse(
                replaceStarWithTrue(nodeFilter),
                validationContext.availableProperties()
            );
            nodeExpression.validate(validationContext).validate();
        } catch (ParseException | SemanticErrors e) {
            throw new IllegalArgumentException("Invalid `nodeFilter` expression.", e);
        }

        Expression relationshipExpression;
        try {
            var validationContext = ValidationContext.forRelationships(graphStore);
            relationshipExpression = ExpressionParser.parse(
                replaceStarWithTrue(relationshipFilter),
                validationContext.availableProperties()
            );
            relationshipExpression.validate(validationContext).validate();
        } catch (ParseException | SemanticErrors e) {
            throw new IllegalArgumentException("Invalid `relationshipFilter` expression.", e);
        }

        return ImmutableExpressions.of(nodeExpression, relationshipExpression);
    }

    private static String replaceStarWithTrue(String filter) {
        return filter.equals(ElementProjection.PROJECT_ALL) ? "true" : filter;
    }

    public static GraphSchema filterSchema(GraphSchema inputGraphSchema, NodesFilter.FilteredNodes filteredNodes, RelationshipsFilter.FilteredRelationships filteredRelationships) {
        var nodeSchema = inputGraphSchema.nodeSchema().filter(filteredNodes.idMap().availableNodeLabels());
        if (nodeSchema.availableLabels().isEmpty()) {
            nodeSchema.addLabel(NodeLabel.ALL_NODES);
        }

        RelationshipSchema relationshipSchema = inputGraphSchema
            .relationshipSchema()
            .filter(filteredRelationships.topology().keySet());
        if (relationshipSchema.availableTypes().isEmpty()) {
            relationshipSchema.addRelationshipType(RelationshipType.ALL_RELATIONSHIPS, Direction.DIRECTED);
        }

        return GraphSchema.of(nodeSchema, relationshipSchema, Map.of());
    }

    private GraphStoreFilter() {}
}
