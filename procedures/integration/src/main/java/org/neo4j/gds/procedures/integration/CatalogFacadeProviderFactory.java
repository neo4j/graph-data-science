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
package org.neo4j.gds.procedures.integration;

import org.neo4j.gds.applications.graphstorecatalog.CatalogBusinessFacade;
import org.neo4j.gds.applications.graphstorecatalog.CatalogConfigurationService;
import org.neo4j.gds.applications.graphstorecatalog.DropGraphApplication;
import org.neo4j.gds.applications.graphstorecatalog.DropNodePropertiesApplication;
import org.neo4j.gds.applications.graphstorecatalog.DropRelationshipsApplication;
import org.neo4j.gds.applications.graphstorecatalog.EstimateCommonNeighbourAwareRandomWalkApplication;
import org.neo4j.gds.applications.graphstorecatalog.GenerateGraphApplication;
import org.neo4j.gds.applications.graphstorecatalog.GraphMemoryUsageApplication;
import org.neo4j.gds.applications.graphstorecatalog.GraphNameValidationService;
import org.neo4j.gds.applications.graphstorecatalog.GraphSamplingApplication;
import org.neo4j.gds.applications.graphstorecatalog.GraphStoreValidationService;
import org.neo4j.gds.applications.graphstorecatalog.ListGraphApplication;
import org.neo4j.gds.applications.graphstorecatalog.NodeLabelMutatorApplication;
import org.neo4j.gds.applications.graphstorecatalog.StreamNodePropertiesApplication;
import org.neo4j.gds.applications.graphstorecatalog.StreamRelationshipPropertiesApplication;
import org.neo4j.gds.applications.graphstorecatalog.StreamRelationshipsApplication;
import org.neo4j.gds.applications.graphstorecatalog.SubGraphProjectApplication;
import org.neo4j.gds.beta.filter.GraphStoreFilterService;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.procedures.KernelTransactionAccessor;
import org.neo4j.gds.procedures.ProcedureTransactionAccessor;
import org.neo4j.gds.procedures.TaskRegistryFactoryService;
import org.neo4j.gds.procedures.TerminationFlagService;
import org.neo4j.gds.procedures.TransactionContextAccessor;
import org.neo4j.gds.services.DatabaseIdAccessor;
import org.neo4j.gds.services.UserAccessor;
import org.neo4j.gds.services.UserLogServices;

import java.util.Optional;
import java.util.function.Function;

public class CatalogFacadeProviderFactory {
    private final Log log;
    private final ExporterBuildersProviderService exporterBuildersProviderService;
    private final Optional<Function<CatalogBusinessFacade, CatalogBusinessFacade>> businessFacadeDecorator;

    public CatalogFacadeProviderFactory(
        Log log,
        ExporterBuildersProviderService exporterBuildersProviderService,
        Optional<Function<CatalogBusinessFacade, CatalogBusinessFacade>> businessFacadeDecorator
    ) {
        this.log = log;
        this.exporterBuildersProviderService = exporterBuildersProviderService;
        this.businessFacadeDecorator = businessFacadeDecorator;
    }

    public CatalogFacadeProvider createCatalogFacadeProvider(
        GraphStoreCatalogService graphStoreCatalogService,
        DatabaseIdAccessor databaseIdAccessor,
        KernelTransactionAccessor kernelTransactionAccessor,
        TaskRegistryFactoryService taskRegistryFactoryService,
        TerminationFlagService terminationFlagService,
        UserLogServices userLogServices,
        UserAccessor userServices
    ) {
        // there are some services that are currently only used for graph catalog, they can live here
        var catalogConfigurationService = new CatalogConfigurationService();
        var graphNameValidationService = new GraphNameValidationService();
        var graphStoreFilterService = new GraphStoreFilterService();
        var graphStoreValidationService = new GraphStoreValidationService();
        var procedureTransactionAccessor = new ProcedureTransactionAccessor();
        var transactionContextAccessor = new TransactionContextAccessor();

        /*
         * The applications capture business logic that is not generally reusable - if it was,
         * it would be domain layer stuff. So we just need to squirrel them away somewhere.
         */
        var dropGraphApplication = new DropGraphApplication(graphStoreCatalogService);
        var dropNodePropertiesApplication = new DropNodePropertiesApplication(log);
        var dropRelationshipsApplication = new DropRelationshipsApplication(log);
        var estimateCommonNeighbourAwareRandomWalkApplication = new EstimateCommonNeighbourAwareRandomWalkApplication();
        var generateGraphApplication = new GenerateGraphApplication(log, graphStoreCatalogService);
        var graphMemoryUsageApplication = new GraphMemoryUsageApplication(graphStoreCatalogService);
        var graphSamplingApplication = new GraphSamplingApplication(log, graphStoreCatalogService);
        var listGraphApplication = new ListGraphApplication(graphStoreCatalogService);
        var nodeLabelMutatorApplication = new NodeLabelMutatorApplication();
        var streamNodePropertiesApplication = new StreamNodePropertiesApplication(log);
        var streamRelationshipPropertiesApplication = new StreamRelationshipPropertiesApplication(log);
        var streamRelationshipsApplication = new StreamRelationshipsApplication();
        var subGraphProjectApplication = new SubGraphProjectApplication(
            log,
            graphStoreFilterService,
            graphStoreCatalogService
        );
        return new CatalogFacadeProvider(
            catalogConfigurationService,
            log,
            graphNameValidationService,
            graphStoreCatalogService,
            graphStoreValidationService,
            procedureTransactionAccessor,
            databaseIdAccessor,
            exporterBuildersProviderService,
            kernelTransactionAccessor,
            taskRegistryFactoryService,
            terminationFlagService,
            transactionContextAccessor,
            userLogServices,
            userServices,
            dropGraphApplication,
            dropNodePropertiesApplication,
            dropRelationshipsApplication,
            estimateCommonNeighbourAwareRandomWalkApplication,
            generateGraphApplication,
            graphMemoryUsageApplication,
            graphSamplingApplication,
            listGraphApplication,
            nodeLabelMutatorApplication,
            streamNodePropertiesApplication,
            streamRelationshipPropertiesApplication,
            streamRelationshipsApplication,
            subGraphProjectApplication,
            businessFacadeDecorator
        );
    }
}
