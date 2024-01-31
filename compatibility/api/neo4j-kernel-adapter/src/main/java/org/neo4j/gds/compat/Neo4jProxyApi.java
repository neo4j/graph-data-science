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
package org.neo4j.gds.compat;

import org.neo4j.common.DependencyResolver;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.FieldSignature;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.procedure.Context;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.procedure.Mode;

import java.util.List;
import java.util.Optional;

public interface Neo4jProxyApi {

    AccessMode accessMode(CustomAccessMode customAccessMode);

    CompatExecutionContext executionContext(KernelTransaction ktx);

    ProcedureSignature procedureSignature(
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
    );

    long estimateNodeCount(Read read, int label);

    long estimateRelationshipCount(Read read, int sourceLabel, int targetLabel, int type);

    CursorContextFactory cursorContextFactory(Optional<PageCacheTracer> pageCacheTracer);

    BoltTransactionRunner<?, ?> boltTransactionRunner();

    boolean isCompositeDatabase(GraphDatabaseService databaseService);

    <T> T lookupComponentProvider(Context ctx, Class<T> component, boolean safe) throws ProcedureException;

    GlobalProcedureRegistry globalProcedureRegistry(GlobalProcedures globalProcedures);

    DependencyResolver emptyDependencyResolver();

    String neo4jArrowServerAddressHeader();

    String metricsManagerClass();
}
