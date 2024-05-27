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
package org.neo4j.gds.procedures;

import org.neo4j.gds.algorithms.similarity.WriteRelationshipService;
import org.neo4j.gds.api.AlgorithmMetaDataSetter;
import org.neo4j.gds.api.GraphLoaderContext;
import org.neo4j.gds.applications.ApplicationsFacade;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmEstimationTemplate;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTemplate;
import org.neo4j.gds.applications.algorithms.machinery.RequestScopedDependencies;
import org.neo4j.gds.applications.graphstorecatalog.CatalogBusinessFacade;
import org.neo4j.gds.configuration.DefaultsConfiguration;
import org.neo4j.gds.configuration.LimitsConfiguration;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.memest.DatabaseGraphStoreEstimationService;
import org.neo4j.gds.metrics.procedures.DeprecatedProceduresMetricService;
import org.neo4j.gds.metrics.projections.ProjectionMetricsService;
import org.neo4j.gds.procedures.algorithms.AlgorithmsProcedureFacade;
import org.neo4j.gds.procedures.algorithms.centrality.CentralityProcedureFacade;
import org.neo4j.gds.procedures.algorithms.configuration.ConfigurationCreator;
import org.neo4j.gds.procedures.algorithms.configuration.ConfigurationParser;
import org.neo4j.gds.procedures.algorithms.runners.EstimationModeRunner;
import org.neo4j.gds.procedures.algorithms.runners.StatsModeAlgorithmRunner;
import org.neo4j.gds.procedures.algorithms.runners.StreamModeAlgorithmRunner;
import org.neo4j.gds.procedures.algorithms.runners.WriteModeAlgorithmRunner;
import org.neo4j.gds.procedures.algorithms.stubs.GenericStub;
import org.neo4j.gds.procedures.catalog.CatalogProcedureFacade;
import org.neo4j.gds.procedures.community.CommunityProcedureFacade;
import org.neo4j.gds.procedures.embeddings.NodeEmbeddingsProcedureFacade;
import org.neo4j.gds.procedures.misc.MiscAlgorithmsProcedureFacade;
import org.neo4j.gds.procedures.pipelines.PipelinesProcedureFacade;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.kernel.api.KernelTransaction;

import java.util.Optional;
import java.util.function.Function;

public class GraphDataScienceProcedures {
    private final Log log;

    private final AlgorithmsProcedureFacade algorithmsProcedureFacade;
    private final CatalogProcedureFacade catalogProcedureFacade;
    private final org.neo4j.gds.procedures.centrality.CentralityProcedureFacade centralityProcedureFacade;
    private final CommunityProcedureFacade communityProcedureFacade;
    private final MiscAlgorithmsProcedureFacade miscAlgorithmsProcedureFacade;
    private final NodeEmbeddingsProcedureFacade nodeEmbeddingsProcedureFacade;
    private final PipelinesProcedureFacade pipelinesProcedureFacade;

    private final DeprecatedProceduresMetricService deprecatedProceduresMetricService;

    /**
     * Keeping this package private to encourage use of @{@link GraphDataScienceProceduresBuilder}
     */
    GraphDataScienceProcedures(
        Log log,
        AlgorithmsProcedureFacade algorithmsProcedureFacade,
        CatalogProcedureFacade catalogProcedureFacade,
        org.neo4j.gds.procedures.centrality.CentralityProcedureFacade centralityProcedureFacade,
        CommunityProcedureFacade communityProcedureFacade,
        MiscAlgorithmsProcedureFacade miscAlgorithmsProcedureFacade,
        NodeEmbeddingsProcedureFacade nodeEmbeddingsProcedureFacade,
        PipelinesProcedureFacade pipelinesProcedureFacade,
        DeprecatedProceduresMetricService deprecatedProceduresMetricService
    ) {
        this.log = log;
        this.algorithmsProcedureFacade = algorithmsProcedureFacade;
        this.catalogProcedureFacade = catalogProcedureFacade;
        this.centralityProcedureFacade = centralityProcedureFacade;
        this.communityProcedureFacade = communityProcedureFacade;
        this.miscAlgorithmsProcedureFacade = miscAlgorithmsProcedureFacade;
        this.nodeEmbeddingsProcedureFacade = nodeEmbeddingsProcedureFacade;
        this.pipelinesProcedureFacade = pipelinesProcedureFacade;
        this.deprecatedProceduresMetricService = deprecatedProceduresMetricService;
    }

    public static GraphDataScienceProcedures create(
        Log log,
        DefaultsConfiguration defaultsConfiguration,
        LimitsConfiguration limitsConfiguration,
        Optional<Function<CatalogBusinessFacade, CatalogBusinessFacade>> catalogBusinessFacadeDecorator,
        GraphStoreCatalogService graphStoreCatalogService,
        ProjectionMetricsService projectionMetricsService,
        AlgorithmMetaDataSetter algorithmMetaDataSetter,
        AlgorithmProcessingTemplate algorithmProcessingTemplate,
        KernelTransaction kernelTransaction,
        GraphLoaderContext graphLoaderContext,
        ProcedureCallContext procedureCallContext,
        RequestScopedDependencies requestScopedDependencies
    ) {
        var configurationParser = new ConfigurationParser(defaultsConfiguration, limitsConfiguration);
        var configurationCreator = new ConfigurationCreator(
            configurationParser,
            algorithmMetaDataSetter,
            requestScopedDependencies.getUser()
        );
        var databaseGraphStoreEstimationService = new DatabaseGraphStoreEstimationService(
            graphLoaderContext,
            requestScopedDependencies.getUser()
        );
        var algorithmEstimationTemplate = new AlgorithmEstimationTemplate(
            graphStoreCatalogService,
            databaseGraphStoreEstimationService,
            requestScopedDependencies
        );
        var genericStub = new GenericStub(
            defaultsConfiguration,
            limitsConfiguration,
            configurationCreator,
            configurationParser,
            requestScopedDependencies.getUser(),
            algorithmEstimationTemplate
        );

        var writeRelationshipService = new WriteRelationshipService(log, requestScopedDependencies);

        var applicationsFacade = ApplicationsFacade.create(
            log,
            catalogBusinessFacadeDecorator,
            graphStoreCatalogService,
            projectionMetricsService,
            algorithmEstimationTemplate,
            algorithmProcessingTemplate,
            requestScopedDependencies,
            writeRelationshipService
        );

        var closeableResourceRegistry = new TransactionCloseableResourceRegistry(kernelTransaction);

        var centralityProcedureFacade = CentralityProcedureFacade.create(
            genericStub,
            applicationsFacade,
            new ProcedureCallContextReturnColumns(procedureCallContext),
            new EstimationModeRunner(configurationCreator),
            new StatsModeAlgorithmRunner(configurationCreator),
            new StreamModeAlgorithmRunner(closeableResourceRegistry, configurationCreator),
            new WriteModeAlgorithmRunner(configurationCreator)
        );

        return new GraphDataScienceProceduresBuilder(log)
            .with(centralityProcedureFacade)
            .build();
    }

    public Log log() {
        return log;
    }

    public AlgorithmsProcedureFacade algorithms() {
        return algorithmsProcedureFacade;
    }

    public CatalogProcedureFacade catalog() {
        return catalogProcedureFacade;
    }

    public org.neo4j.gds.procedures.centrality.CentralityProcedureFacade centrality() {
        return centralityProcedureFacade;
    }

    public CommunityProcedureFacade community() {
        return communityProcedureFacade;
    }

    public MiscAlgorithmsProcedureFacade miscellaneousAlgorithms() {
        return miscAlgorithmsProcedureFacade;
    }

    public NodeEmbeddingsProcedureFacade nodeEmbeddings() {
        return nodeEmbeddingsProcedureFacade;
    }

    public PipelinesProcedureFacade pipelines() {
        return pipelinesProcedureFacade;
    }

    public DeprecatedProceduresMetricService deprecatedProcedures() {
        return deprecatedProceduresMetricService;
    }
}
