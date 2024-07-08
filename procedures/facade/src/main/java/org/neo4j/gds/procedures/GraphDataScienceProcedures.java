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

import org.neo4j.gds.api.AlgorithmMetaDataSetter;
import org.neo4j.gds.api.ProcedureReturnColumns;
import org.neo4j.gds.applications.ApplicationsFacade;
import org.neo4j.gds.applications.algorithms.embeddings.GraphSageModelRepository;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTemplate;
import org.neo4j.gds.applications.algorithms.machinery.MemoryGuard;
import org.neo4j.gds.applications.algorithms.machinery.RequestScopedDependencies;
import org.neo4j.gds.applications.algorithms.machinery.WriteContext;
import org.neo4j.gds.applications.graphstorecatalog.CatalogBusinessFacade;
import org.neo4j.gds.configuration.DefaultsConfiguration;
import org.neo4j.gds.configuration.LimitsConfiguration;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.metrics.algorithms.AlgorithmMetricsService;
import org.neo4j.gds.metrics.procedures.DeprecatedProceduresMetricService;
import org.neo4j.gds.metrics.projections.ProjectionMetricsService;
import org.neo4j.gds.procedures.algorithms.AlgorithmsProcedureFacade;
import org.neo4j.gds.procedures.algorithms.configuration.ConfigurationCreator;
import org.neo4j.gds.procedures.algorithms.configuration.ConfigurationParser;
import org.neo4j.gds.procedures.catalog.CatalogProcedureFacade;
import org.neo4j.gds.procedures.misc.MiscAlgorithmsProcedureFacade;
import org.neo4j.gds.procedures.pipelines.PipelinesProcedureFacade;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.KernelTransaction;

import java.util.Optional;
import java.util.function.Function;

public class GraphDataScienceProcedures {
    private final Log log;

    private final AlgorithmsProcedureFacade algorithmsProcedureFacade;
    private final CatalogProcedureFacade catalogProcedureFacade;
    private final MiscAlgorithmsProcedureFacade miscAlgorithmsProcedureFacade;
    private final PipelinesProcedureFacade pipelinesProcedureFacade;

    private final DeprecatedProceduresMetricService deprecatedProceduresMetricService;

    /**
     * Keeping this package private to encourage use of @{@link GraphDataScienceProceduresBuilder}
     */
    GraphDataScienceProcedures(
        Log log,
        AlgorithmsProcedureFacade algorithmsProcedureFacade,
        CatalogProcedureFacade catalogProcedureFacade,
        MiscAlgorithmsProcedureFacade miscAlgorithmsProcedureFacade,
        PipelinesProcedureFacade pipelinesProcedureFacade,
        DeprecatedProceduresMetricService deprecatedProceduresMetricService
    ) {
        this.log = log;
        this.algorithmsProcedureFacade = algorithmsProcedureFacade;
        this.catalogProcedureFacade = catalogProcedureFacade;
        this.miscAlgorithmsProcedureFacade = miscAlgorithmsProcedureFacade;
        this.pipelinesProcedureFacade = pipelinesProcedureFacade;
        this.deprecatedProceduresMetricService = deprecatedProceduresMetricService;
    }

    public static GraphDataScienceProcedures create(
        Log log,
        DefaultsConfiguration defaultsConfiguration,
        LimitsConfiguration limitsConfiguration,
        Optional<Function<AlgorithmProcessingTemplate, AlgorithmProcessingTemplate>> algorithmProcessingTemplateDecorator,
        Optional<Function<CatalogBusinessFacade, CatalogBusinessFacade>> catalogBusinessFacadeDecorator,
        GraphStoreCatalogService graphStoreCatalogService,
        MemoryGuard memoryGuard,
        AlgorithmMetricsService algorithmMetricsService,
        ProjectionMetricsService projectionMetricsService,
        AlgorithmMetaDataSetter algorithmMetaDataSetter,
        KernelTransaction kernelTransaction,
        RequestScopedDependencies requestScopedDependencies,
        WriteContext writeContext,
        ProcedureReturnColumns procedureReturnColumns,
        CatalogProcedureFacadeFactory catalogProcedureFacadeFactory,
        GraphDatabaseService graphDatabaseService,
        Transaction transaction,
        AlgorithmProcedureFacadeBuilderFactory algorithmProcedureFacadeBuilderFactory,
        DeprecatedProceduresMetricService deprecatedProceduresMetricService,
        ModelCatalog modelCatalog,
        GraphSageModelRepository graphSageModelRepository
    ) {
        var configurationParser = new ConfigurationParser(defaultsConfiguration, limitsConfiguration);
        var configurationCreator = new ConfigurationCreator(
            configurationParser,
            algorithmMetaDataSetter,
            requestScopedDependencies.getUser()
        );

        var applicationsFacade = ApplicationsFacade.create(
            log,
            algorithmProcessingTemplateDecorator,
            catalogBusinessFacadeDecorator,
            graphStoreCatalogService,
            memoryGuard,
            algorithmMetricsService,
            projectionMetricsService,
            requestScopedDependencies,
            writeContext,
            modelCatalog,
            graphSageModelRepository
        );

        var catalogProcedureFacade = catalogProcedureFacadeFactory.createCatalogProcedureFacade(
            applicationsFacade,
            graphDatabaseService,
            kernelTransaction,
            transaction,
            requestScopedDependencies,
            writeContext,
            procedureReturnColumns
        );

        var algorithmProcedureFacadeBuilder = algorithmProcedureFacadeBuilderFactory.create(
            configurationParser,
            configurationCreator,
            requestScopedDependencies,
            kernelTransaction,
            algorithmMetaDataSetter,
            applicationsFacade,
            writeContext,
            procedureReturnColumns
        );

        var centralityProcedureFacade = algorithmProcedureFacadeBuilder.createCentralityProcedureFacade();
        var communityProcedureFacade = algorithmProcedureFacadeBuilder.createCommunityProcedureFacade();
        var miscAlgorithmsProcedureFacade = algorithmProcedureFacadeBuilder.createMiscellaneousProcedureFacade();
        var nodeEmbeddingsProcedureFacade = algorithmProcedureFacadeBuilder.createNodeEmbeddingsProcedureFacade();
        var pathFindingProcedureFacade = algorithmProcedureFacadeBuilder.createPathFindingProcedureFacade();
        var similarityProcedureFacade = algorithmProcedureFacadeBuilder.createSimilarityProcedureFacade();

        var pipelinesProcedureFacade = new PipelinesProcedureFacade(requestScopedDependencies.getUser());

        return new GraphDataScienceProceduresBuilder(log)
            .with(catalogProcedureFacade)
            .with(centralityProcedureFacade)
            .with(communityProcedureFacade)
            .with(miscAlgorithmsProcedureFacade)
            .with(nodeEmbeddingsProcedureFacade)
            .with(pathFindingProcedureFacade)
            .with(pipelinesProcedureFacade)
            .with(similarityProcedureFacade)
            .with(deprecatedProceduresMetricService)
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

    public MiscAlgorithmsProcedureFacade miscellaneousAlgorithms() {
        return miscAlgorithmsProcedureFacade;
    }

    public PipelinesProcedureFacade pipelines() {
        return pipelinesProcedureFacade;
    }

    public DeprecatedProceduresMetricService deprecatedProcedures() {
        return deprecatedProceduresMetricService;
    }
}
