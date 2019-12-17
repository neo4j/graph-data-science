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

import org.neo4j.graphalgo.api.GraphSetup;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.utils.TransactionWrapper;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Map;
import java.util.function.Consumer;

public interface ProcTestBaseExtensions {

    default <
        A extends Algorithm<A, ?>,
        P extends LegacyAlgoBaseProc<A, ?>,
        F extends AlgorithmFactory<A, ProcedureConfiguration>>
    void getAlgorithmFactory(
        Class<? extends P> procClazz,
        GraphDatabaseAPI db,
        String nodeLabels,
        String relTypes,
        Map<String, Object> config,
        Consumer<F> func
    ) {
        getAlgorithmProc(procClazz, db, proc -> {
            ProcedureConfiguration procedureConfiguration = proc.newConfig(nodeLabels, relTypes, config);
            func.accept((F) proc.algorithmFactory(procedureConfiguration));
        });
    }

    default <
        A extends Algorithm<A, ?>,
        P extends LegacyAlgoBaseProc<A, ?>>
    void getGraphSetup(
        Class<? extends P> procClazz,
        GraphDatabaseAPI db,
        String nodeLabels,
        String relTypes,
        Map<String, Object> config,
        Consumer<GraphSetup> func
    ) {
        getAlgorithmProc(procClazz, db, (proc) -> {
            ProcedureConfiguration procedureConfiguration = proc.newConfig(nodeLabels, relTypes, config);
            func.accept(proc.getGraphLoader(procedureConfiguration, AllocationTracker.EMPTY).toSetup());
        });
    }

    default <
        A extends Algorithm<A, ?>,
        P extends AlgoBaseProc<A, ?, ProcedureConfiguration>>
    void getAlgorithmProc(
        Class<? extends P> procClazz,
        GraphDatabaseAPI db,
        Consumer<P> func
    ) {
        new TransactionWrapper(db).accept((tx -> {
            P proc;
            try {
                proc = procClazz.newInstance();
            } catch (ReflectiveOperationException e) {
                throw new RuntimeException("Could not instantiate Procedure Class " + procClazz.getSimpleName());
            }

            proc.transaction = tx;
            proc.api = db;
            proc.callContext = ProcedureCallContext.EMPTY;
            proc.log = new TestLog();

            func.accept(proc);
        }));
    }
}
