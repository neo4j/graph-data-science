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
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.gds.core.utils.warnings.EmptyUserLogRegistryFactory;
import org.neo4j.gds.core.utils.warnings.UserLogRegistryFactory;
import org.neo4j.gds.metrics.MetricsFacade;
import org.neo4j.gds.procedures.GraphDataScience;
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
        Username username,
        MetricsFacade metricsFacade,
        GraphDataScience graphDataScience
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

        proc.metricsFacade = metricsFacade;
        proc.graphDataScience = graphDataScience;

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
            username,
            MetricsFacade.PASSTHROUGH_METRICS_FACADE,
            /*
             * So, procedure runner needs _something_ to satisfy some down stream code. Null is not good enough,
             * I already tried but failed. At the same time,
             * it is clear that _probably_ we can get away with less than the full gamut,
             * just like the other dependencies here that are faked out. Honestly, the whole thing is big, unwieldy,
             *  and largely opaque to me.
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
            new GraphDataScience(
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null
            ) {
                @Override
                public String toString() {
                    return "from Procedure Runner"; // handy for debugging
                }
            }
        );
        func.accept(proc);
        return proc;
    }
}
