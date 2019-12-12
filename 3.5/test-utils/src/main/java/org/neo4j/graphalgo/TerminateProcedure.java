/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
package org.neo4j.graphalgo;

import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;

public class TerminateProcedure {

    @Context
    public GraphDatabaseAPI api;

    @Context
    public Log log;

    @Context
    public KernelTransaction transaction;

    @Procedure("test.testProc")
    public void allShortestPathsStream(
            @Name(value = "config", defaultValue = "{}")
                    Map<String, Object> config) {

        final TerminationFlag flag = TerminationFlag.wrap(transaction);
        while (flag.running()) {
            // simulate long running algorithm
            try {
                Thread.sleep(100);
            } catch (InterruptedException ignore) {
            }
        }
        log.info("algorithm termination successful");
    }

    public static class Result {
        public final long id;

        public Result(long id) {
            this.id = id;
        }
    }
}
