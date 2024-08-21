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
package org.neo4j.gds.applications;

import org.neo4j.gds.algorithms.similarity.WriteRelationshipService;
import org.neo4j.gds.applications.algorithms.centrality.CentralityApplications;
import org.neo4j.gds.applications.algorithms.community.CommunityApplications;
import org.neo4j.gds.applications.algorithms.embeddings.NodeEmbeddingApplications;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmEstimationTemplate;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTemplate;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTemplateConvenience;
import org.neo4j.gds.applications.algorithms.machinery.DefaultAlgorithmProcessingTemplate;
import org.neo4j.gds.applications.algorithms.machinery.MemoryGuard;
import org.neo4j.gds.applications.algorithms.machinery.MutateNodeProperty;
import org.neo4j.gds.applications.algorithms.machinery.ProgressTrackerCreator;
import org.neo4j.gds.applications.algorithms.machinery.RequestScopedDependencies;
import org.neo4j.gds.applications.algorithms.machinery.WriteContext;
import org.neo4j.gds.applications.algorithms.miscellaneous.MiscellaneousApplications;
import org.neo4j.gds.applications.algorithms.pathfinding.PathFindingApplications;
import org.neo4j.gds.applications.algorithms.similarity.SimilarityApplications;
import org.neo4j.gds.applications.graphstorecatalog.DefaultGraphCatalogApplications;
import org.neo4j.gds.applications.graphstorecatalog.ExportLocation;
import org.neo4j.gds.applications.graphstorecatalog.GraphCatalogApplications;
import org.neo4j.gds.applications.modelcatalog.DefaultModelCatalogApplications;
import org.neo4j.gds.applications.modelcatalog.ModelCatalogApplications;
import org.neo4j.gds.applications.modelcatalog.ModelRepository;
import org.neo4j.gds.applications.operations.OperationsApplications;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.memest.DatabaseGraphStoreEstimationService;
import org.neo4j.gds.metrics.algorithms.AlgorithmMetricsService;
import org.neo4j.gds.metrics.projections.ProjectionMetricsService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;

import java.util.Optional;
import java.util.function.Function;

/**
 * This is the top level facade for GDS applications. If you are integrating GDS,
 * this is the one thing you want to work with. See for example Neo4j Procedures.
 * <p>
 * We use the facade pattern for well known reasons,
 * and we apply a breakdown into sub-facades to keep things smaller and more manageable.
 */
public final class ApplicationsFacade {
    private final CentralityApplications centralityApplications;
    private final CommunityApplications communityApplications;
    private final GraphCatalogApplications graphCatalogApplications;
    private final MiscellaneousApplications miscellaneousApplications;
    private final ModelCatalogApplications modelCatalogApplications;
    private final NodeEmbeddingApplications nodeEmbeddingApplications;
    private final OperationsApplications operationsApplications;
    private final PathFindingApplications pathFindingApplications;
    private final SimilarityApplications similarityApplications;

    ApplicationsFacade(
        CentralityApplications centralityApplications,
        CommunityApplications communityApplications,
        GraphCatalogApplications graphCatalogApplications,
        MiscellaneousApplications miscellaneousApplications,
        ModelCatalogApplications modelCatalogApplications,
        NodeEmbeddingApplications nodeEmbeddingApplications,
        OperationsApplications operationsApplications,
        PathFindingApplications pathFindingApplications,
        SimilarityApplications similarityApplications
    ) {
        this.centralityApplications = centralityApplications;
        this.communityApplications = communityApplications;
        this.graphCatalogApplications = graphCatalogApplications;
        this.miscellaneousApplications = miscellaneousApplications;
        this.modelCatalogApplications = modelCatalogApplications;
        this.nodeEmbeddingApplications = nodeEmbeddingApplications;
        this.operationsApplications = operationsApplications;
        this.pathFindingApplications = pathFindingApplications;
        this.similarityApplications = similarityApplications;
    }

    /**
     * We can stuff all the boring structure stuff in here so nobody needs to worry about it.
     */
    public static ApplicationsFacade create(
        Log log,
        ExportLocation exportLocation,
        Optional<Function<AlgorithmProcessingTemplate, AlgorithmProcessingTemplate>> algorithmProcessingTemplateDecorator,
        Optional<Function<GraphCatalogApplications, GraphCatalogApplications>> graphCatalogApplicationsDecorator,
        Optional<Function<ModelCatalogApplications, ModelCatalogApplications>> modelCatalogApplicationsDecorator,
        GraphStoreCatalogService graphStoreCatalogService,
        MemoryGuard memoryGuard,
        AlgorithmMetricsService algorithmMetricsService,
        ProjectionMetricsService projectionMetricsService,
        RequestScopedDependencies requestScopedDependencies,
        WriteContext writeContext,
        ModelCatalog modelCatalog,
        ModelRepository modelRepository,
        GraphDatabaseService graphDatabaseService,
        Transaction procedureTransaction
    ) {
        var databaseGraphStoreEstimationService = new DatabaseGraphStoreEstimationService(
            requestScopedDependencies.getGraphLoaderContext(),
            requestScopedDependencies.getUser()
        );
        var algorithmEstimationTemplate = new AlgorithmEstimationTemplate(
            graphStoreCatalogService,
            databaseGraphStoreEstimationService,
            requestScopedDependencies
        );

        var algorithmProcessingTemplate = createAlgorithmProcessingTemplate(
            log,
            algorithmProcessingTemplateDecorator,
            graphStoreCatalogService,
            memoryGuard,
            algorithmMetricsService,
            requestScopedDependencies
        );
        var algorithmProcessingTemplateConvenience = new AlgorithmProcessingTemplateConvenience(algorithmProcessingTemplate);

        var progressTrackerCreator = new ProgressTrackerCreator(log, requestScopedDependencies);

        var mutateNodeProperty = new MutateNodeProperty(log);

        var centralityApplications = CentralityApplications.create(
            log,
            requestScopedDependencies,
            writeContext,
            algorithmEstimationTemplate,
            algorithmProcessingTemplateConvenience,
            progressTrackerCreator,
            mutateNodeProperty
        );

        var communityApplications = CommunityApplications.create(
            log,
            requestScopedDependencies,
            writeContext,
            algorithmEstimationTemplate,
            algorithmProcessingTemplateConvenience,
            progressTrackerCreator,
            mutateNodeProperty
        );

        var graphCatalogApplications = createGraphCatalogApplications(
            log,
            exportLocation,
            graphStoreCatalogService,
            projectionMetricsService,
            requestScopedDependencies,
            graphDatabaseService,
            procedureTransaction,
            graphCatalogApplicationsDecorator
        );

        var miscellaneousApplications = MiscellaneousApplications.create(
            log,
            requestScopedDependencies,
            writeContext,
            algorithmEstimationTemplate,
            algorithmProcessingTemplateConvenience,
            progressTrackerCreator,
            mutateNodeProperty
        );

        var modelCatalogApplications = createModelCatalogApplications(
            requestScopedDependencies,
            modelCatalog,
            modelCatalogApplicationsDecorator
        );

        var nodeEmbeddingApplications = NodeEmbeddingApplications.create(
            log,
            requestScopedDependencies,
            writeContext,
            algorithmEstimationTemplate,
            algorithmProcessingTemplateConvenience,
            progressTrackerCreator,
            mutateNodeProperty,
            modelCatalog,
            modelRepository
        );

        var operationsApplications = OperationsApplications.create(requestScopedDependencies);

        var pathFindingApplications = PathFindingApplications.create(
            log,
            requestScopedDependencies,
            writeContext,
            algorithmEstimationTemplate,
            algorithmProcessingTemplateConvenience,
            progressTrackerCreator
        );

        var writeRelationshipService = new WriteRelationshipService(log, requestScopedDependencies, writeContext);

        var similarityApplications = SimilarityApplications.create(
            log,
            requestScopedDependencies,
            algorithmEstimationTemplate,
            algorithmProcessingTemplateConvenience,
            progressTrackerCreator,
            writeRelationshipService
        );

        return new ApplicationsFacadeBuilder()
            .with(centralityApplications)
            .with(communityApplications)
            .with(graphCatalogApplications)
            .with(miscellaneousApplications)
            .with(modelCatalogApplications)
            .with(nodeEmbeddingApplications)
            .with(operationsApplications)
            .with(pathFindingApplications)
            .with(similarityApplications)
            .build();
    }

    private static AlgorithmProcessingTemplate createAlgorithmProcessingTemplate(
        Log log,
        Optional<Function<AlgorithmProcessingTemplate, AlgorithmProcessingTemplate>> algorithmProcessingTemplateDecorator,
        GraphStoreCatalogService graphStoreCatalogService,
        MemoryGuard memoryGuard,
        AlgorithmMetricsService algorithmMetricsService,
        RequestScopedDependencies requestScopedDependencies
    ) {
        var algorithmProcessingTemplate = new DefaultAlgorithmProcessingTemplate(
            log,
            algorithmMetricsService,
            graphStoreCatalogService,
            memoryGuard,
            requestScopedDependencies
        );

        if (algorithmProcessingTemplateDecorator.isEmpty()) return algorithmProcessingTemplate;

        return algorithmProcessingTemplateDecorator.get().apply(algorithmProcessingTemplate);
    }

    private static GraphCatalogApplications createGraphCatalogApplications(
        Log log,
        ExportLocation exportLocation,
        GraphStoreCatalogService graphStoreCatalogService,
        ProjectionMetricsService projectionMetricsService,
        RequestScopedDependencies requestScopedDependencies,
        GraphDatabaseService graphDatabaseService,
        Transaction procedureTransaction,
        Optional<Function<GraphCatalogApplications, GraphCatalogApplications>> graphCatalogApplicationsDecorator
    ) {
        var graphCatalogApplications = DefaultGraphCatalogApplications.create(
            log,
            exportLocation,
            graphStoreCatalogService,
            projectionMetricsService,
            requestScopedDependencies,
            graphDatabaseService,
            procedureTransaction
        );

        if (graphCatalogApplicationsDecorator.isEmpty()) return graphCatalogApplications;

        return graphCatalogApplicationsDecorator.get().apply(graphCatalogApplications);
    }

    private static ModelCatalogApplications createModelCatalogApplications(
        RequestScopedDependencies requestScopedDependencies,
        ModelCatalog modelCatalog,
        Optional<Function<ModelCatalogApplications, ModelCatalogApplications>> modelCatalogApplicationsDecorator
    ) {
        var modelCatalogApplications = DefaultModelCatalogApplications.create(
            modelCatalog,
            requestScopedDependencies.getUser()
        );

        if (modelCatalogApplicationsDecorator.isEmpty()) return modelCatalogApplications;

        return modelCatalogApplicationsDecorator.get().apply(modelCatalogApplications);
    }

    public CentralityApplications centrality() {
        return centralityApplications;
    }

    public CommunityApplications community() {
        return communityApplications;
    }

    public GraphCatalogApplications graphCatalog() {
        return graphCatalogApplications;
    }

    public MiscellaneousApplications miscellaneous() {
        return miscellaneousApplications;
    }

    public ModelCatalogApplications modelCatalog() {
        return modelCatalogApplications;
    }

    public NodeEmbeddingApplications nodeEmbeddings() {
        return nodeEmbeddingApplications;
    }

    public OperationsApplications operations() {
        return operationsApplications;
    }

    public PathFindingApplications pathFinding() {
        return pathFindingApplications;
    }

    public SimilarityApplications similarity() {
        return similarityApplications;
    }
}
