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
package org.neo4j.gds.projection;

import org.neo4j.gds.annotation.CustomProcedure;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.compat.GraphDatabaseApiProxy;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.compat.UserFunctionSignatureBuilder;
import org.neo4j.gds.core.loading.Capabilities.WriteMode;
import org.neo4j.gds.core.utils.progress.TaskStore;
import org.neo4j.gds.logging.LogAdapter;
import org.neo4j.gds.metrics.Metrics;
import org.neo4j.gds.transaction.DatabaseTransactionContext;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.Neo4jTypes;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.internal.kernel.api.procs.UserAggregationReducer;
import org.neo4j.internal.kernel.api.procs.UserFunctionSignature;
import org.neo4j.kernel.api.procedure.CallableUserAggregationFunction;
import org.neo4j.kernel.api.procedure.Context;
import org.neo4j.kernel.database.DatabaseReferenceImpl;
import org.neo4j.kernel.database.DatabaseReferenceRepository;
import org.neo4j.kernel.impl.api.KernelTransactions;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Name;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.TextValue;

import static org.neo4j.internal.kernel.api.procs.DefaultParameterValue.nullValue;
import static org.neo4j.internal.kernel.api.procs.FieldSignature.inputField;

public class CypherAggregation implements CallableUserAggregationFunction {

    // NOTE: keep in sync with `procedureSyntax`
    static final QualifiedName FUNCTION_NAME = new QualifiedName(
        new String[]{"gds", "graph"},
        "project"
    );

    // NOTE: keep in sync with `procedureSyntax`
    @Override
    public UserFunctionSignature signature() {
        return UserFunctionSignatureBuilder.builder()
            .name(FUNCTION_NAME)
            .addInputField(inputField("graphName", Neo4jTypes.NTString))
            .addInputField(inputField("sourceNode", Neo4jTypes.NTAny))
            .addInputField(inputField("targetNode", Neo4jTypes.NTAny, nullValue(Neo4jTypes.NTAny)))
            .addInputField(inputField("dataConfig", Neo4jTypes.NTAny, nullValue(Neo4jTypes.NTAny)))
            .addInputField(inputField("configuration", Neo4jTypes.NTAny, nullValue(Neo4jTypes.NTAny)))
            .addInputField(inputField("alphaMigrationConfig", Neo4jTypes.NTAny, nullValue(Neo4jTypes.NTAny)))
            .returnType(Neo4jTypes.NTMap)
            .description("Creates a named graph in the catalog for use by algorithms.")
            .threadSafe(true) // yes, please
            .build().toNeo();
    }

    // NOTE: keep in sync with `FUNCTION_NAME` and `signature`
    @CustomProcedure(value = "gds.graph.project", namespace = CustomProcedure.Namespace.AGGREGATION_FUNCTION)
    public AggregationResult procedureSyntax(
        @Name("graphName") TextValue graphName,
        @Name("sourceNode") AnyValue sourceNode,
        @Name("targetNode") AnyValue targetNode,
        @Name("dataConfig") AnyValue dataConfig,
        @Name("configuration") AnyValue config
    ) {
        throw new UnsupportedOperationException("This method is only used to document the procedure syntax.");
    }

    @Override
    public UserAggregationReducer createReducer(Context ctx) throws ProcedureException {
        var databaseService = ctx.graphDatabaseAPI();
        var metrics = Neo4jProxy.lookupComponentProvider(ctx, Metrics.class, true);
        var taskStore = Neo4jProxy.lookupComponentProvider(ctx, TaskStore.class, true);
        var log = Neo4jProxy.lookupComponentProvider(ctx, Log.class, true);
        var username = ctx.kernelTransaction().securityContext().subject().executingUser();
        var transaction = ctx.transaction();

        var databaseId = GraphDatabaseApiProxy.databaseId(databaseService);
        var repo = GraphDatabaseApiProxy.resolveDependency(databaseService, DatabaseReferenceRepository.class);
        var runsOnCompositeDatabase = repo.getCompositeDatabaseReferences()
            .stream()
            .map(DatabaseReferenceImpl.Internal::databaseId)
            .anyMatch(databaseId::equals);

        ExecutingQueryProvider queryProvider;
        if (runsOnCompositeDatabase) {
            queryProvider = ExecutingQueryProvider.empty();
        } else {
            assert GraphDatabaseApiProxy.containsDependency(databaseService, KernelTransactions.class);
            var ktxs = GraphDatabaseApiProxy.resolveDependency(databaseService, KernelTransactions.class);
            queryProvider = ExecutingQueryProvider.fromTransaction(ktxs, transaction);
        }

        var writeMode = runsOnCompositeDatabase
            ? WriteMode.NONE
            : WriteMode.LOCAL;

        var queryEstimator = QueryEstimator.fromTransaction(DatabaseTransactionContext.of(databaseService, transaction));

        return new ProductGraphAggregator(
            DatabaseId.of(databaseService.databaseName()),
            username,
            writeMode,
            queryEstimator,
            queryProvider,
            metrics.projectionMetrics(),
            taskStore,
            new LogAdapter(log)
        );
    }
}
