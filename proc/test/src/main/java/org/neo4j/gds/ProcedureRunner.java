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
package org.neo4j.gds;

import org.neo4j.gds.api.AlgorithmMetaDataSetter;
import org.neo4j.gds.api.GraphLoaderContext;
import org.neo4j.gds.api.User;
import org.neo4j.gds.applications.algorithms.machinery.MemoryGuard;
import org.neo4j.gds.applications.algorithms.machinery.RequestScopedDependencies;
import org.neo4j.gds.applications.algorithms.machinery.WriteContext;
import org.neo4j.gds.compat.GraphDatabaseApiProxy;
import org.neo4j.gds.configuration.DefaultsConfiguration;
import org.neo4j.gds.configuration.LimitsConfiguration;
import org.neo4j.gds.core.Username;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.gds.core.model.OpenModelCatalog;
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.gds.core.utils.warnings.EmptyUserLogRegistryFactory;
import org.neo4j.gds.core.utils.warnings.EmptyUserLogStore;
import org.neo4j.gds.core.utils.warnings.UserLogRegistryFactory;
import org.neo4j.gds.logging.LogAdapter;
import org.neo4j.gds.metrics.MetricsFacade;
import org.neo4j.gds.metrics.PassthroughExecutionMetricRegistrar;
import org.neo4j.gds.metrics.algorithms.AlgorithmMetricsService;
import org.neo4j.gds.metrics.procedures.DeprecatedProceduresMetricService;
import org.neo4j.gds.procedures.AlgorithmProcedureFacadeBuilderFactory;
import org.neo4j.gds.procedures.CatalogProcedureFacadeFactory;
import org.neo4j.gds.procedures.DatabaseIdAccessor;
import org.neo4j.gds.procedures.GraphDataScienceProcedures;
import org.neo4j.gds.procedures.ProcedureCallContextReturnColumns;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.logging.Log;

import java.util.Optional;
import java.util.function.Consumer;

public final class ProcedureRunner {

    private ProcedureRunner() {}

    @SuppressWarnings("WeakerAccess") // needs to be public, or a test gets unhappy
    public static <P extends BaseProc> P instantiateProcedure(
        GraphDatabaseService databaseService,
        Class<P> procClass,
        ProcedureCallContext procedureCallContext,
        Log log,
        TaskRegistryFactory taskRegistryFactory,
        UserLogRegistryFactory userLogRegistryFactory,
        Transaction tx,
        KernelTransaction kernelTransaction,
        Username username,
        MetricsFacade metricsFacade,
        GraphDataScienceProcedures graphDataScienceProcedures
    ) {
        P proc;
        try {
            proc = procClass.getDeclaredConstructor().newInstance();
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException("Could not instantiate Procedure Class " + procClass.getSimpleName(), e);
        }

        proc.procedureTransaction = tx;
        proc.transaction = kernelTransaction;
        proc.databaseService = databaseService;
        proc.callContext = procedureCallContext;
        proc.log = log;
        proc.taskRegistryFactory = taskRegistryFactory;
        proc.userLogRegistryFactory = userLogRegistryFactory;
        proc.username = username;

        proc.metricsFacade = metricsFacade;
        proc.graphDataScienceProcedures = graphDataScienceProcedures;

        return proc;
    }

    public static <P extends BaseProc> P applyOnProcedure(
        GraphDatabaseService databaseService,
        Class<P> procClass,
        ProcedureCallContext procedureCallContext,
        Log log,
        TaskRegistryFactory taskRegistryFactory,
        Transaction tx,
        Username username,
        Consumer<P> func
    ) {
        var kernelTransaction = GraphDatabaseApiProxy.kernelTransaction(tx);

        var graphDataScienceProcedures = createGraphDataScienceProcedures(
            log,
            kernelTransaction,
            databaseService,
            procedureCallContext,
            taskRegistryFactory,
            tx,
            username
        );

        var proc = instantiateProcedure(
            databaseService,
            procClass,
            procedureCallContext,
            log,
            taskRegistryFactory,
            EmptyUserLogRegistryFactory.INSTANCE,
            tx,
            kernelTransaction,
            username,
            MetricsFacade.PASSTHROUGH_METRICS_FACADE,
            graphDataScienceProcedures
        );
        func.accept(proc);
        return proc;
    }

    /**
     * I'm going to go with a blanket statement of, if you run into problems with this,
     * it is because you are trying to test some intricacies way down in the bowels of GDS,
     * but driving from the top of Neo4j Procedures, and that this creates a very unhelpful coupling.
     * At this juncture, better to move/ formulate those tests at the local level of the code you care about,
     * instead of trying to fake up a big massive stack.
     * Everything on the inside of this GDS facade is meant to be simple POJO style,
     * direct dependency injected code that is easy to new up and in turn fake out,
     * so testing granular bits should be a doozy.
     * Happy to be proved wrong, so come talk to me if you get in trouble ;)
     */
    private static GraphDataScienceProcedures createGraphDataScienceProcedures(
        Log log,
        KernelTransaction kernelTransaction,
        GraphDatabaseService graphDatabaseService,
        ProcedureCallContext procedureCallContext,
        TaskRegistryFactory taskRegistryFactory,
        Transaction procedureTransaction,
        Username username
    ) {
        var gdsLog = new LogAdapter(log);

        var procedureContext = WriteContext.builder()
            .build();

        var requestScopedDependencies = RequestScopedDependencies.builder()
            .with(new DatabaseIdAccessor().getDatabaseId(graphDatabaseService))
            .with(GraphLoaderContext.NULL_CONTEXT)
            .with(taskRegistryFactory)
            .with(new User(username.username(), false))
            .with(EmptyUserLogRegistryFactory.INSTANCE)
            .with(EmptyUserLogStore.INSTANCE)
            .build();
        var graphStoreCatalogService = new GraphStoreCatalogService();

        var catalogProcedureFacadeFactory = new CatalogProcedureFacadeFactory(gdsLog);

        var modelCatalog = new OpenModelCatalog();

        var algorithmFacadeBuilderFactory = new AlgorithmProcedureFacadeBuilderFactory(
            gdsLog,
            DefaultsConfiguration.Instance,
            LimitsConfiguration.Instance,
            graphStoreCatalogService,
            false,
            new AlgorithmMetricsService(new PassthroughExecutionMetricRegistrar())
        );

        return GraphDataScienceProcedures.create(
            gdsLog,
            DefaultsConfiguration.Instance,
            LimitsConfiguration.Instance,
            Optional.empty(),
            Optional.empty(),
            graphStoreCatalogService,
            MemoryGuard.DISABLED,
            MetricsFacade.PASSTHROUGH_METRICS_FACADE.algorithmMetrics(),
            MetricsFacade.PASSTHROUGH_METRICS_FACADE.projectionMetrics(),
            AlgorithmMetaDataSetter.EMPTY,
            kernelTransaction,
            requestScopedDependencies,
            procedureContext,
            new ProcedureCallContextReturnColumns(procedureCallContext),
            catalogProcedureFacadeFactory,
            graphDatabaseService,
            procedureTransaction,
            algorithmFacadeBuilderFactory,
            DeprecatedProceduresMetricService.PASSTHROUGH,
            modelCatalog,
            null
        );
    }
}
