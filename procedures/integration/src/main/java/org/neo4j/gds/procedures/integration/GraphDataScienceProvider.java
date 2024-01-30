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

import org.neo4j.function.ThrowingFunction;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.metrics.procedures.DeprecatedProceduresMetricService;
import org.neo4j.gds.procedures.GraphDataScience;
import org.neo4j.gds.procedures.GraphDataScienceBuilder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.procedure.Context;

/**
 * We use this at request time to construct the facade that the procedures call.
 */
public class GraphDataScienceProvider implements ThrowingFunction<Context, GraphDataScience, ProcedureException> {
    private final Log log;
    private final CatalogFacadeProvider catalogFacadeProvider;
    private final AlgorithmFacadeProviderFactory algorithmFacadeService;
    private final DeprecatedProceduresMetricService deprecatedProceduresMetricService;

    GraphDataScienceProvider(
        Log log,
        CatalogFacadeProvider catalogFacadeProvider,
        AlgorithmFacadeProviderFactory algorithmFacadeService,
        DeprecatedProceduresMetricService deprecatedProceduresMetricService
    ) {
        this.log = log;
        this.catalogFacadeProvider = catalogFacadeProvider;
        this.algorithmFacadeService = algorithmFacadeService;
        this.deprecatedProceduresMetricService = deprecatedProceduresMetricService;
    }

    @Override
    public GraphDataScience apply(Context context) throws ProcedureException {

        var catalogFacade = catalogFacadeProvider.createCatalogFacade(context);
        context.dependencyResolver().resolveDependency(GraphDatabaseService.class);
        var algorithmFacadeProvider = algorithmFacadeService.createAlgorithmFacadeProvider(context);

        var centralityProcedureFacade = algorithmFacadeProvider.createCentralityProcedureFacade();
        var communityProcedureFacade = algorithmFacadeProvider.createCommunityProcedureFacade();
        var pathFindingProcedureFacade = algorithmFacadeProvider.createPathFindingProcedureFacade();
        var similarityProcedureFacade = algorithmFacadeProvider.createSimilarityProcedureFacade();
        var nodeEmbeddingsProcedureFacade = algorithmFacadeProvider.createNodeEmbeddingsProcedureFacade();

        return new GraphDataScienceBuilder(log)
            .with(catalogFacade)
            .with(centralityProcedureFacade)
            .with(communityProcedureFacade)
            .with(nodeEmbeddingsProcedureFacade)
            .with(similarityProcedureFacade)
            .with(pathFindingProcedureFacade)
            .with(deprecatedProceduresMetricService)
            .build();
    }
}
