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

import org.neo4j.gds.api.ProcedureReturnColumns;
import org.neo4j.gds.applications.ApplicationsFacade;
import org.neo4j.gds.applications.algorithms.machinery.RequestScopedDependencies;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.gds.procedures.algorithms.configuration.UserSpecificConfigurationParser;
import org.neo4j.gds.procedures.algorithms.stubs.GenericStub;
import org.neo4j.kernel.api.KernelTransaction;

public class AlgorithmProcedureFacadeBuilderFactory {

    private final GraphStoreCatalogService graphStoreCatalogService;

    public AlgorithmProcedureFacadeBuilderFactory(
        GraphStoreCatalogService graphStoreCatalogService
    ) {

        this.graphStoreCatalogService = graphStoreCatalogService;
    }

    AlgorithmProcedureFacadeBuilder create(
        UserSpecificConfigurationParser configurationParser,
        RequestScopedDependencies requestScopedDependencies,
        KernelTransaction kernelTransaction,
        ApplicationsFacade applicationsFacade,
        ProcedureReturnColumns procedureReturnColumns
    ) {
        var nodeLookup = new TransactionNodeLookup(kernelTransaction);
        var closeableResourceRegistry = new TransactionCloseableResourceRegistry(kernelTransaction);

        var genericStub = GenericStub.create(
            graphStoreCatalogService,
            configurationParser,
            requestScopedDependencies
        );


        return new AlgorithmProcedureFacadeBuilder(
            requestScopedDependencies,
            closeableResourceRegistry,
            nodeLookup,
            procedureReturnColumns,
            applicationsFacade,
            genericStub,
            configurationParser
        );
    }
}
