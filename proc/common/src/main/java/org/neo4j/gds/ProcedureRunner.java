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

import org.neo4j.gds.compat.GraphDatabaseApiProxy;
import org.neo4j.gds.core.Username;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.gds.core.utils.warnings.EmptyUserLogRegistryFactory;
import org.neo4j.gds.core.utils.warnings.UserLogRegistryFactory;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.logging.Log;

import java.util.function.Consumer;

public final class ProcedureRunner {

    private ProcedureRunner() {}

    public static <P extends BaseProc> P instantiateProcedure(
        GraphDatabaseService databaseService,
        Class<P> procClass,
        ProcedureCallContext procedureCallContext,
        Log log,
        TaskRegistryFactory taskRegistryFactory,
        UserLogRegistryFactory userLogRegistryFactory,
        Transaction tx,
        Username username
    ) {
        P proc;
        try {
            proc = procClass.getDeclaredConstructor().newInstance();
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException("Could not instantiate Procedure Class " + procClass.getSimpleName(), e);
        }

        proc.procedureTransaction = tx;
        proc.transaction = GraphDatabaseApiProxy.kernelTransaction(tx);
        proc.databaseService = databaseService;
        proc.callContext = procedureCallContext;
        proc.log = log;
        proc.taskRegistryFactory = taskRegistryFactory;
        proc.userLogRegistryFactory = userLogRegistryFactory;
        proc.username = username;
        proc.internalModelCatalog = GraphDatabaseApiProxy.resolveDependency(proc.databaseService, ModelCatalog.class);

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
        var proc = instantiateProcedure(
            databaseService,
            procClass,
            procedureCallContext,
            log,
            taskRegistryFactory,
            EmptyUserLogRegistryFactory.INSTANCE,
            tx,
            username
        );
        func.accept(proc);
        return proc;
    }
}
