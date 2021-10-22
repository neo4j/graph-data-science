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
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;

import java.util.function.Consumer;

public final class ProcedureRunner {

    private ProcedureRunner() {}

    public static <P extends BaseProc> P applyOnProcedure(
        GraphDatabaseAPI graphDb,
        Class<P> procClass,
        ProcedureCallContext procedureCallContext,
        Log log,
        TaskRegistryFactory taskRegistryFactory,
        Consumer<P> func
    ) {
        return GraphDatabaseApiProxy.applyInTransaction(graphDb, tx -> {
            P proc;
            try {
                proc = procClass.getDeclaredConstructor().newInstance();
            } catch (ReflectiveOperationException e) {
                throw new RuntimeException("Could not instantiate Procedure Class " + procClass.getSimpleName());
            }

            proc.procedureTransaction = tx;
            proc.transaction = GraphDatabaseApiProxy.kernelTransaction(tx);
            proc.api = graphDb;
            proc.callContext = procedureCallContext;
            proc.log = log;
            proc.taskRegistryFactory = taskRegistryFactory;

            func.accept(proc);
            return proc;
        });
    }
}
