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
package org.neo4j.gds.applications.graphstorecatalog;

import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.config.GraphProjectConfig;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.gds.core.utils.warnings.UserLogRegistryFactory;
import org.neo4j.gds.cypherprojection.GraphProjectCypherResult;
import org.neo4j.gds.cypherprojection.GraphProjectFromCypherConfig;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.gds.transaction.TransactionContext;
import org.neo4j.graphdb.GraphDatabaseService;

public class CypherProjectApplication {
    private final GenericProjectApplication<
        GraphProjectCypherResult,
        GraphProjectFromCypherConfig,
        GraphProjectCypherResult.Builder> genericProjectApplication;

    public CypherProjectApplication(
        GenericProjectApplication<
            GraphProjectCypherResult,
            GraphProjectFromCypherConfig,
            GraphProjectCypherResult.Builder> genericProjectApplication
    ) {
        this.genericProjectApplication = genericProjectApplication;
    }

    public GraphProjectCypherResult project(
        DatabaseId databaseId,
        GraphDatabaseService graphDatabaseService,
        GraphProjectMemoryUsageService graphProjectMemoryUsageService,
        TaskRegistryFactory taskRegistryFactory,
        TerminationFlag terminationFlag,
        TransactionContext transactionContext,
        UserLogRegistryFactory userLogRegistryFactory,
        GraphProjectFromCypherConfig configuration
    ) {
        return genericProjectApplication.project(
            databaseId,
            graphDatabaseService,
            graphProjectMemoryUsageService,
            taskRegistryFactory,
            terminationFlag,
            transactionContext,
            userLogRegistryFactory,
            configuration
        );
    }

    public MemoryEstimateResult estimate(
        DatabaseId databaseId,
        GraphProjectMemoryUsageService graphProjectMemoryUsageService,
        TaskRegistryFactory taskRegistryFactory,
        TerminationFlag terminationFlag,
        TransactionContext transactionContext,
        UserLogRegistryFactory userLogRegistryFactory,
        GraphProjectConfig configuration
    ) {
        if (configuration.isFictitiousLoading()) return estimateButFictitiously(
            graphProjectMemoryUsageService,
            configuration
        );

        var memoryTreeWithDimensions = graphProjectMemoryUsageService.getEstimate(
            databaseId,
            terminationFlag,
            transactionContext,
            taskRegistryFactory,
            userLogRegistryFactory,
            configuration
        );

        return new MemoryEstimateResult(memoryTreeWithDimensions);
    }

    /**
     * Public because EstimationCLI tests needs it. Should redesign something here I think
     */
    public MemoryEstimateResult estimateButFictitiously(
        GraphProjectMemoryUsageService graphProjectMemoryUsageService,
        GraphProjectConfig configuration
    ) {
        var estimate = graphProjectMemoryUsageService.getFictitiousEstimate(configuration);

        return new MemoryEstimateResult(estimate);
    }
}
