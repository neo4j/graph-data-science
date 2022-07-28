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
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.core.Username;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.logging.Log;

import java.util.function.Consumer;

public final class TestProcedureRunner {

    private TestProcedureRunner() {}

    public static <P extends BaseProc> void applyOnProcedure(
        GraphDatabaseService graphDb,
        Class<P> procClass,
        Consumer<P> func
    ) {
        applyOnProcedure(graphDb, procClass, Neo4jProxy.testLog(), func);
    }

    public static <P extends BaseProc> void applyOnProcedure(
        GraphDatabaseService graphDb,
        Class<P> procClass,
        Log log,
        Consumer<P> func
    ) {
        GraphDatabaseApiProxy.runInTransaction(graphDb, tx -> ProcedureRunner.applyOnProcedure(
            graphDb,
            procClass,
            ProcedureCallContext.EMPTY,
            log,
            EmptyTaskRegistryFactory.INSTANCE,
            tx,
            Username.EMPTY_USERNAME,
            func
        ));
    }
}
