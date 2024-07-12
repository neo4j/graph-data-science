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
import org.neo4j.gds.applications.algorithms.machinery.RequestScopedDependencies;
import org.neo4j.gds.configuration.DefaultsConfiguration;
import org.neo4j.gds.configuration.LimitsConfiguration;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.gds.procedures.algorithms.configuration.ConfigurationCreator;
import org.neo4j.gds.procedures.algorithms.configuration.ConfigurationParser;
import org.neo4j.gds.procedures.algorithms.runners.DefaultAlgorithmExecutionScaffolding;
import org.neo4j.gds.procedures.algorithms.runners.EstimationModeRunner;
import org.neo4j.gds.procedures.algorithms.runners.MetadataSetter;
import org.neo4j.gds.procedures.algorithms.stubs.GenericStub;
import org.neo4j.kernel.api.KernelTransaction;

public class AlgorithmProcedureFacadeBuilderFactory {
    private final DefaultsConfiguration defaultsConfiguration;
    private final LimitsConfiguration limitsConfiguration;
    private final GraphStoreCatalogService graphStoreCatalogService;

    public AlgorithmProcedureFacadeBuilderFactory(
        DefaultsConfiguration defaultsConfiguration,
        LimitsConfiguration limitsConfiguration,
        GraphStoreCatalogService graphStoreCatalogService
    ) {
        this.defaultsConfiguration = defaultsConfiguration;
        this.limitsConfiguration = limitsConfiguration;
        this.graphStoreCatalogService = graphStoreCatalogService;
    }

    AlgorithmProcedureFacadeBuilder create(
        ConfigurationParser configurationParser,
        ConfigurationCreator configurationCreator,
        RequestScopedDependencies requestScopedDependencies,
        KernelTransaction kernelTransaction,
        AlgorithmMetaDataSetter algorithmMetaDataSetter,
        ApplicationsFacade applicationsFacade,
        ProcedureReturnColumns procedureReturnColumns
    ) {
        var nodeLookup = new TransactionNodeLookup(kernelTransaction);
        var closeableResourceRegistry = new TransactionCloseableResourceRegistry(kernelTransaction);

        var genericStub = GenericStub.create(
            defaultsConfiguration,
            limitsConfiguration,
            graphStoreCatalogService,
            configurationCreator,
            configurationParser,
            requestScopedDependencies
        );

        var estimationModeRunner = new EstimationModeRunner(configurationCreator);
        var algorithmExecutionScaffolding = new DefaultAlgorithmExecutionScaffolding(configurationCreator);
        var algorithmExecutionScaffoldingForStreamMode = new MetadataSetter(
            algorithmMetaDataSetter,
            algorithmExecutionScaffolding
        );

        return new AlgorithmProcedureFacadeBuilder(
            requestScopedDependencies,
            closeableResourceRegistry,
            nodeLookup,
            procedureReturnColumns,
            applicationsFacade,
            genericStub,
            estimationModeRunner,
            algorithmExecutionScaffolding,
            algorithmExecutionScaffoldingForStreamMode
        );
    }
}
