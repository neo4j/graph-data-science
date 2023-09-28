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
import org.neo4j.gds.applications.graphstorecatalog.GraphNameValidationService;
import org.neo4j.gds.applications.graphstorecatalog.GraphStoreValidationService;
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

        return new CatalogFacadeProvider(
            catalogConfigurationService,
            log,
            graphNameValidationService,
            graphStoreCatalogService,
            graphStoreFilterService,
            graphStoreValidationService,
            procedureTransactionAccessor,
            databaseIdAccessor,
            exporterBuildersProviderService, // we always just offer native writes in OpenGDS
            kernelTransactionAccessor,
            taskRegistryFactoryService,
            terminationFlagService,
            transactionContextAccessor,
            userLogServices,
            userServices,
            businessFacadeDecorator // we have no extra checks to do in OpenGDS
        );
    }
}
