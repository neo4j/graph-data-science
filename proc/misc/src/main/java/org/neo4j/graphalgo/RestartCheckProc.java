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
package org.neo4j.graphalgo;

import org.neo4j.collection.RawIterator;
import org.neo4j.graphalgo.compat.GraphDatabaseApiProxy;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;
import org.neo4j.graphalgo.core.model.ModelCatalog;
import org.neo4j.graphalgo.core.utils.progress.ProgressEventStore;
import org.neo4j.procedure.Context;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.FieldSignature;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.kernel.api.ResourceTracker;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.procedure.CallableProcedure;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Procedure;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.BooleanValue;

import java.util.Collections;
import java.util.List;

import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTBoolean;

public class RestartCheckProc {

    private static boolean compute(ProgressEventStore progress) {
        return GraphStoreCatalog.isEmpty() && ModelCatalog.isEmpty() && progress.isEmpty();
    }

    @Context
    public ProgressEventStore progress;

    @Context
    public GraphDatabaseService db;

    @Procedure("gds.internal.enableProc")
    public void enableProc() {
        var globalProcedures = GraphDatabaseApiProxy.resolveDependency(db, GlobalProcedures.class);
        try {
            globalProcedures.procedure(LazyProcedure.PROCEDURE_NAME);
            // already registered
            return;
        } catch (ProcedureException e) {
            if (e.status() != Status.Procedure.ProcedureNotFound) {
                throw new RuntimeException(e.getMessage(), e);
            }
        }
        try {
            globalProcedures.register(new LazyProcedure(progress), false);
        } catch (ProcedureException e) {
            throw new RuntimeException("Could not register procedure: " + e.getMessage(), e);
        }
    }

    private static final class LazyProcedure implements CallableProcedure {
        private static final QualifiedName PROCEDURE_NAME = new QualifiedName(
            new String[]{"gds", "internal"},
            "safeToRestart"
        );

        private static final ProcedureSignature SIGNATURE = new ProcedureSignature(
            PROCEDURE_NAME,
            // input signature: ()
            List.of(),
            // output signature: a single boolean field, named safeToRestart
            List.of(FieldSignature.outputField("safeToRestart", NTBoolean)),
            // procedure mode
            Mode.READ,
            // require admin user for execution
            true,
            // no deprecation
            null,
            // empty allow - related to advanced procedure permissions
            new String[0],
            // procedure description
            "We tell you when it is safe to restart this node.",
            // no warning
            null,
            // not eager - Unclear what the effect is
            false,
            // case sensitive name match
            false,
            // Procedure is not allowed to be run on the system database
            false,
            // hide from dbms.procedures listing
            true
        );

        private final ProgressEventStore progress;

        private LazyProcedure(ProgressEventStore progress) {
            this.progress = progress;
        }

        @Override
        public ProcedureSignature signature() {
            return SIGNATURE;
        }

        @Override
        public RawIterator<AnyValue[], ProcedureException> apply(
            org.neo4j.kernel.api.procedure.Context ctx,
            AnyValue[] input,
            ResourceTracker resourceTracker
        ) {
            var value = compute(progress) ? BooleanValue.TRUE : BooleanValue.FALSE;
            return RawIterator.wrap(Collections.singletonList(new AnyValue[]{value}).iterator());
        }
    }
}
