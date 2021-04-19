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
package org.neo4j.gds.internal;

import org.neo4j.graphalgo.compat.Neo4jProxy;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;
import org.neo4j.graphalgo.core.model.ModelCatalog;
import org.neo4j.graphalgo.core.utils.progress.ProgressEventStore;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.internal.kernel.api.procs.UserFunctionSignature;
import org.neo4j.kernel.api.procedure.CallableUserFunction;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.Values;

import java.util.List;

import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTBoolean;

public class AuraMaintenanceFunction implements CallableUserFunction {

    private static boolean compute(ProgressEventStore progress) {
        return GraphStoreCatalog.isEmpty() && ModelCatalog.isEmpty() && progress.isEmpty();
    }

    private static final QualifiedName PROCEDURE_NAME = new QualifiedName(
        new String[]{"gds", "internal"},
        "safeToRestart"
    );

    private static final UserFunctionSignature SIGNATURE = Neo4jProxy.userFunctionSignature(
        PROCEDURE_NAME,
        // input signature: ()
        List.of(),
        // output type: return a boolean
        NTBoolean,
        // no deprecation
        null,
        // empty allow - related to advanced function permissions
        new String[0],
        // function description
        "We tell you when it is safe to restart this node.",
        // No predefined categpry (like temporal or math)
        null,
        // case sensitive name match
        false
    );

    AuraMaintenanceFunction() {
    }

    @Override
    public AnyValue apply(org.neo4j.kernel.api.procedure.Context ctx, AnyValue[] input) throws ProcedureException {
        var progressEventStore = InternalProceduresUtil.lookup(ctx, ProgressEventStore.class);
        return Values.booleanValue(compute(progressEventStore));
    }

    @Override
    public boolean threadSafe() {
        return true;
    }

    @Override
    public UserFunctionSignature signature() {
        return SIGNATURE;
    }
}
