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

import org.neo4j.gds.ElementProjection;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.NodeProjection;
import org.neo4j.gds.NodeProjections;
import org.neo4j.gds.RelationshipProjection;
import org.neo4j.gds.RelationshipProjections;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.CSRGraphStoreFactory;
import org.neo4j.gds.api.GraphLoaderContext;
import org.neo4j.gds.config.GraphProjectFromCypherConfig;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.ImmutableGraphDimensions;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.TaskProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.TaskTreeProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.core.utils.warnings.EmptyUserLogRegistryFactory;
import org.neo4j.gds.transaction.TransactionContext;

import javax.annotation.Nullable;

import static org.neo4j.gds.core.loading.CypherQueryEstimator.EstimationResult;
import static org.neo4j.internal.kernel.api.security.AccessMode.Static.READ;

public final class CypherFactory extends CSRGraphStoreFactory<GraphProjectFromCypherConfig> {

    private final GraphProjectFromCypherConfig cypherConfig;
    private final EstimationResult nodeEstimation;
    private final EstimationResult relationshipEstimation;
    private final ProgressTracker progressTracker;

    public static CypherFactory createWithBaseDimensions(GraphProjectFromCypherConfig graphProjectConfig, GraphLoaderContext loadingContext, GraphDimensions graphDimensions) {
        return create(graphProjectConfig, loadingContext, graphDimensions);
    }

    public static CypherFactory createWithDerivedDimensions(GraphProjectFromCypherConfig graphProjectConfig, GraphLoaderContext loadingContext) {
        return create(graphProjectConfig, loadingContext, null);
    }

    private static CypherFactory create(
        GraphProjectFromCypherConfig graphProjectConfig,
        GraphLoaderContext loadingContext,
        @Nullable GraphDimensions dimensions
    ) {
        var estimator = new CypherQueryEstimator(loadingContext.transactionContext().withRestrictedAccess(READ));

        EstimationResult nodeQueryEstimation = graphProjectConfig.isFictitiousLoading()
            ? ImmutableEstimationResult.of(graphProjectConfig.nodeCount(), 0)
            : estimator.getNodeEstimation(graphProjectConfig.nodeQuery());

        EstimationResult relationshipQueryEstimation = graphProjectConfig.isFictitiousLoading()
            ? ImmutableEstimationResult.of(graphProjectConfig.relationshipCount(), 0)
            : estimator.getRelationshipEstimation(graphProjectConfig.relationshipQuery());

        var dimBuilder = ImmutableGraphDimensions.builder();

        if (dimensions != null) {
            dimBuilder.from(dimensions);
        }

        GraphDimensions dim = ImmutableGraphDimensions.builder()
            .highestPossibleNodeCount(nodeQueryEstimation.estimatedRows())
            .nodeCount(nodeQueryEstimation.estimatedRows())
            .relCountUpperBound(relationshipQueryEstimation.estimatedRows())
            .build();

        return new CypherFactory(
            graphProjectConfig,
            loadingContext,
            dim,
            nodeQueryEstimation,
            relationshipQueryEstimation
        );
    }

    private CypherFactory(
        GraphProjectFromCypherConfig graphProjectConfig,
        GraphLoaderContext loadingContext,
        GraphDimensions graphDimensions,
        EstimationResult nodeEstimation,
        EstimationResult relationshipEstimation

    ) {
        // TODO: need to pass capabilities from outside?
        super(graphProjectConfig, ImmutableStaticCapabilities.of(true), loadingContext, graphDimensions);

        this.cypherConfig = graphProjectConfig;
        this.nodeEstimation = nodeEstimation;
        this.relationshipEstimation = relationshipEstimation;
        this.progressTracker = initProgressTracker();
    }

    @Override
    protected ProgressTracker progressTracker() {
        return progressTracker;
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
            .highestPossibleNodeCount(Math.max(dimensions.highestPossibleNodeCount(), nodeEstimation.estimatedRows()))
            .nodeCount(Math.max(dimensions.nodeCount(), nodeEstimation.estimatedRows()))
            .relCountUpperBound(Math.max(dimensions.relCountUpperBound(), relationshipEstimation.estimatedRows()))
            .build();
    }

    @Override
    public CSRGraphStore build() {
        // Temporarily override the security context to enforce read-only access during load
        return readOnlyTransaction().apply((tx, ktx) -> {
            BatchLoadResult nodeCount = new CountingCypherRecordLoader(
                cypherConfig.nodeQuery(),
                CypherRecordLoader.QueryType.NODE,
                cypherConfig,
                loadingContext
            ).load(ktx.internalTransaction());

            progressTracker.beginSubTask("Loading");
            var nodes = new CypherNodeLoader(
                cypherConfig.nodeQuery(),
                nodeCount.rows(),
                cypherConfig,
                loadingContext,
                progressTracker
            ).load(ktx.internalTransaction());

            var relationshipImportResult = new CypherRelationshipLoader(
                cypherConfig.relationshipQuery(),
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

    private ProgressTracker initProgressTracker() {
        var estimatedDimensions = estimationDimensions();
        var task = Tasks.task(
            "Loading",
            Tasks.leaf("Nodes", estimatedDimensions.highestPossibleNodeCount()),
            Tasks.leaf("Relationships", estimatedDimensions.relCountUpperBound())
        );

        if (graphProjectConfig.logProgress()) {
            return new TaskProgressTracker(
                task,
                loadingContext.log(),
                graphProjectConfig.readConcurrency(),
                graphProjectConfig.jobId(),
                loadingContext.taskRegistryFactory(),
                EmptyUserLogRegistryFactory.INSTANCE
            );
        }

        return new TaskTreeProgressTracker(
            task,
            loadingContext.log(),
            graphProjectConfig.readConcurrency(),
            graphProjectConfig.jobId(),
            loadingContext.taskRegistryFactory(),
            EmptyUserLogRegistryFactory.INSTANCE
        );
    }

    private TransactionContext readOnlyTransaction() {
        return loadingContext.transactionContext().withRestrictedAccess(READ);
    }

    private NodeProjections buildEstimateNodeProjections() {
        var nodeProjection = NodeProjection
            .builder()
            .label(ElementProjection.PROJECT_ALL)
            .addAllProperties(nodeEstimation.propertyMappings())
            .build();

        return NodeProjections.single(
            NodeLabel.ALL_NODES,
            nodeProjection
        );
    }

    private RelationshipProjections buildEstimateRelationshipProjections() {
        var relationshipProjection = RelationshipProjection
            .builder()
            .type(ElementProjection.PROJECT_ALL)
            .addAllProperties(relationshipEstimation.propertyMappings())
            .build();

        return RelationshipProjections.single(
            RelationshipType.ALL_RELATIONSHIPS,
            relationshipProjection
        );
    }
}
