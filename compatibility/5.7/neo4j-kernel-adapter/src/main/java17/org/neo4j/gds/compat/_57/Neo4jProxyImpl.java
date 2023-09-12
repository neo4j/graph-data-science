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
package org.neo4j.gds.compat._57;

import org.neo4j.gds.compat.BoltTransactionRunner;
import org.neo4j.gds.compat.GlobalProcedureRegistry;
import org.neo4j.gds.compat._5x.CommonNeo4jProxyImpl;
import org.neo4j.internal.kernel.api.procs.FieldSignature;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.internal.kernel.api.procs.UserFunctionSignature;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.context.EmptyVersionContextSupplier;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.procedure.Mode;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

public final class Neo4jProxyImpl extends CommonNeo4jProxyImpl {

    @Override
    public BoltTransactionRunner<?, ?> boltTransactionRunner() {
        return new BoltTransactionRunnerImpl();
    }

    @Override
    public ProcedureSignature procedureSignature(
        QualifiedName name,
        List<FieldSignature> inputSignature,
        List<FieldSignature> outputSignature,
        Mode mode,
        boolean admin,
        String deprecated,
        String description,
        String warning,
        boolean eager,
        boolean caseInsensitive,
        boolean systemProcedure,
        boolean internal,
        boolean allowExpiredCredentials,
        boolean threadSafe
    ) {
        return new ProcedureSignature(
            name,
            inputSignature,
            outputSignature,
            mode,
            admin,
            deprecated,
            description,
            warning,
            eager,
            caseInsensitive,
            systemProcedure,
            internal,
            allowExpiredCredentials
        );
    }

    @Override
    public GlobalProcedureRegistry globalProcedureRegistry(GlobalProcedures globalProcedures) {
        return new GlobalProcedureRegistry() {
            @Override
            public Set<ProcedureSignature> getAllProcedures() {
                return globalProcedures.getAllProcedures();
            }

            @Override
            public Stream<UserFunctionSignature> getAllNonAggregatingFunctions() {
                return globalProcedures.getAllNonAggregatingFunctions();
            }

            @Override
            public Stream<UserFunctionSignature> getAllAggregatingFunctions() {
                return globalProcedures.getAllAggregatingFunctions();
            }
        };
    }

    @Override
    public CursorContextFactory cursorContextFactory(Optional<PageCacheTracer> pageCacheTracer) {
        return pageCacheTracer.map(cacheTracer -> new CursorContextFactory(
            cacheTracer,
            EmptyVersionContextSupplier.EMPTY
        )).orElse(CursorContextFactory.NULL_CONTEXT_FACTORY);
    }
}
