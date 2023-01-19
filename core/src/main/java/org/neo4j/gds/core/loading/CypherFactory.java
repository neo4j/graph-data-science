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
import org.neo4j.gds.ElementProjection;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.NodeProjection;
import org.neo4j.gds.NodeProjections;
import org.neo4j.gds.PropertyMapping;
import org.neo4j.gds.RelationshipProjection;
import org.neo4j.gds.RelationshipProjections;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.CSRGraphStoreFactory;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.GraphLoaderContext;
import org.neo4j.gds.api.schema.GraphSchema;
import org.neo4j.gds.compat.GraphDatabaseApiProxy;
import org.neo4j.gds.config.GraphProjectConfig;
import org.neo4j.gds.config.GraphProjectFromCypherConfig;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.GraphDimensionsCypherReader;
import org.neo4j.gds.core.ImmutableGraphDimensions;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.TaskProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.core.utils.warnings.EmptyUserLogRegistryFactory;
import org.neo4j.gds.transaction.TransactionContext;
import org.neo4j.internal.id.IdGeneratorFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;
import static org.neo4j.internal.kernel.api.security.AccessMode.Static.READ;
import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_PROPERTY_KEY;

public class CypherFactory extends CSRGraphStoreFactory<GraphProjectFromCypherConfig> {

    private final GraphProjectFromCypherConfig cypherConfig;
    private EstimationResult nodeEstimation;
    private EstimationResult relationshipEstimation;

    public CypherFactory(
        GraphProjectFromCypherConfig graphProjectConfig,
        GraphLoaderContext loadingContext
    ) {
        this(
            graphProjectConfig,
            loadingContext,
            new GraphDimensionsCypherReader(
                loadingContext.transactionContext().withRestrictedAccess(READ),
                graphProjectConfig,
                GraphDatabaseApiProxy.resolveDependency(loadingContext.graphDatabaseService(), IdGeneratorFactory.class)
            ).call()
        );
    }

    public CypherFactory(
        GraphProjectFromCypherConfig graphProjectConfig,
        GraphLoaderContext loadingContext,
        GraphDimensions graphDimensions
    ) {
        // TODO: need to pass capabilities from outside?
        super(graphProjectConfig, ImmutableStaticCapabilities.of(true), loadingContext, graphDimensions);
        this.cypherConfig = getCypherConfig(graphProjectConfig).orElseThrow(() -> new IllegalArgumentException(
            "Expected GraphProjectConfig to be a cypher config."));
    }

    @Override
    public final MemoryEstimation estimateMemoryUsageDuringLoading() {
        return NativeFactory.getMemoryEstimation(
            buildEstimateNodeProjections(),
            buildEstimateRelationshipProjections(),
            true
        );
    }

    @Override
    public MemoryEstimation estimateMemoryUsageAfterLoading() {
        return NativeFactory.getMemoryEstimation(
            buildEstimateNodeProjections(),
            buildEstimateRelationshipProjections(),
            false
        );
    }

    @Override
    public GraphDimensions estimationDimensions() {
        return ImmutableGraphDimensions.builder()
            .from(dimensions)
            .highestPossibleNodeCount(getNodeEstimation().estimatedRows())
            .nodeCount(getNodeEstimation().estimatedRows())
            .relCountUpperBound(getRelationshipEstimation().estimatedRows())
            .build();
    }

    @Override
    protected GraphSchema computeGraphSchema(
        Nodes nodes, RelationshipImportResult relationshipImportResult
    ) {
        return CSRGraphStoreUtil.computeGraphSchema(
            nodes,
            relationshipImportResult
        );
    }

    @Override
    public CSRGraphStore build() {
        // Temporarily override the security context to enforce read-only access during load
        return readOnlyTransaction().apply((tx, ktx) -> {
            BatchLoadResult nodeCount = new CountingCypherRecordLoader(
                nodeQuery(),
                CypherRecordLoader.QueryType.NODE,
                cypherConfig,
                loadingContext
            ).load(ktx.internalTransaction());

            progressTracker.beginSubTask("Loading");
            var nodes = new CypherNodeLoader(
                nodeQuery(),
                nodeCount.rows(),
                cypherConfig,
                loadingContext,
                progressTracker
            ).load(ktx.internalTransaction());

            var relationshipImportResult = new CypherRelationshipLoader(
                relationshipQuery(),
                nodes.idMap(),
                cypherConfig,
                loadingContext,
                progressTracker
            ).load(ktx.internalTransaction());

            var graphStore = createGraphStore(
                nodes,
                relationshipImportResult
            );

            progressTracker.endSubTask("Loading");

            logLoadingSummary(graphStore);

            return graphStore;
        });
    }

    @Override
    protected ProgressTracker initProgressTracker() {
        var task = Tasks.task(
            "Loading",
            Tasks.leaf("Nodes"),
            Tasks.leaf("Relationships", dimensions.relCountUpperBound())
        );
        return new TaskProgressTracker(
            task,
            loadingContext.log(),
            graphProjectConfig.readConcurrency(),
            graphProjectConfig.jobId(),
            loadingContext.taskRegistryFactory(),
            EmptyUserLogRegistryFactory.INSTANCE
        );
    }

    private String nodeQuery() {
        return getCypherConfig(graphProjectConfig)
            .orElseThrow(() -> new IllegalArgumentException("Missing node query"))
            .nodeQuery();
    }

    private String relationshipQuery() {
        return getCypherConfig(graphProjectConfig)
            .orElseThrow(() -> new IllegalArgumentException("Missing relationship query"))
            .relationshipQuery();
    }

    private static Optional<GraphProjectFromCypherConfig> getCypherConfig(GraphProjectConfig config) {
        if (config instanceof GraphProjectFromCypherConfig) {
            return Optional.of((GraphProjectFromCypherConfig) config);
        }
        return Optional.empty();
    }

    private TransactionContext readOnlyTransaction() {
        return loadingContext.transactionContext().withRestrictedAccess(READ);
    }

    private EstimationResult getNodeEstimation() {
        if (nodeEstimation == null) {
            nodeEstimation = runEstimationQuery(
                nodeQuery(),
                NodeSubscriber.RESERVED_COLUMNS
            );
        }
        return nodeEstimation;
    }

    private EstimationResult getRelationshipEstimation() {
        if (relationshipEstimation == null) {
            relationshipEstimation = runEstimationQuery(
                relationshipQuery(),
                RelationshipSubscriber.RESERVED_COLUMNS
            );
        }
        return relationshipEstimation;
    }

    private EstimationResult runEstimationQuery(String query, Collection<String> reservedColumns) {
        return readOnlyTransaction().apply((tx, ktx) -> {
            var explainQuery = formatWithLocale("EXPLAIN %s", query);
            try (var result = tx.execute(explainQuery)) {
                var estimatedRows = (Number) result.getExecutionPlanDescription().getArguments().get("EstimatedRows");

                var propertyColumns = new ArrayList<>(result.columns());
                propertyColumns.removeAll(reservedColumns);

                return ImmutableEstimationResult.of(estimatedRows.longValue(), propertyColumns.size());
            }
        });
    }

    private NodeProjections buildEstimateNodeProjections() {
        if (cypherConfig.isFictitiousLoading()) {
            nodeEstimation = ImmutableEstimationResult.of(cypherConfig.nodeCount(), 0);
        }

        var nodeProjection = NodeProjection
            .builder()
            .label(ElementProjection.PROJECT_ALL)
            .addAllProperties(getNodeEstimation().propertyMappings())
            .build();

        return NodeProjections.single(
            NodeLabel.ALL_NODES,
            nodeProjection
        );
    }

    private RelationshipProjections buildEstimateRelationshipProjections() {
        if (cypherConfig.isFictitiousLoading()) {
            relationshipEstimation = ImmutableEstimationResult.of(cypherConfig.relationshipCount(), 0);
        }

        var relationshipProjection = RelationshipProjection
            .builder()
            .type(ElementProjection.PROJECT_ALL)
            .addAllProperties(getRelationshipEstimation().propertyMappings())
            .build();

        return RelationshipProjections.single(
            RelationshipType.ALL_RELATIONSHIPS,
            relationshipProjection
        );
    }

    @ValueClass
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
