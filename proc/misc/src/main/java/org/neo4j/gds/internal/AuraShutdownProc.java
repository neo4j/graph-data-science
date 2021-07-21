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

import org.neo4j.collection.RawIterator;
import org.neo4j.configuration.Config;
import org.neo4j.graphalgo.compat.GraphStoreExportSettings;
import org.neo4j.graphalgo.compat.Neo4jProxy;
import org.neo4j.graphalgo.core.loading.CatalogRequest;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;
import org.neo4j.graphalgo.core.model.ModelCatalog;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.DefaultParameterValue;
import org.neo4j.internal.kernel.api.procs.FieldSignature;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.kernel.api.ResourceTracker;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.procedure.CallableProcedure;
import org.neo4j.kernel.api.procedure.Context;
import org.neo4j.logging.Log;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.NumberValue;
import org.neo4j.values.storable.Values;

import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import static org.neo4j.internal.helpers.collection.Iterators.asRawIterator;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTBoolean;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTInteger;
import static org.neo4j.procedure.Mode.READ;

public class AuraShutdownProc implements CallableProcedure {

    private static final QualifiedName PROCEDURE_NAME = new QualifiedName(
        new String[]{"gds", "internal"},
        "shutdown"
    );

    private static final ProcedureSignature SIGNATURE = Neo4jProxy.procedureSignature(
        PROCEDURE_NAME,
        // Input signature: [
        //   @Name(value = "timeoutInSeconds", defaultValue = "42") long timeoutInSeconds
        // ]
        List.of(FieldSignature.inputField("timeoutInSeconds", NTInteger, DefaultParameterValue.ntInteger(42))),
        // Output type: return a boolean
        List.of(FieldSignature.outputField("done", NTBoolean)),
        // Procedure mode
        READ,
        // Do not require admin user for execution
        false,
        // No deprecation
        null,
        // Roles that are explicitly allowed to call this procedure (none)
        new String[0],
        // Procedure description
        "Prepare for a showdown of the DBMS.",
        // No warning
        null,
        // eagerness
        false,
        // case sensitive name match
        false,
        // Do not allow procedure on the system db
        false,
        // Hide procedure from listings (@Internal)
        true,
        // Require valid credentials
        false
    );

    @Override
    public ProcedureSignature signature() {
        return SIGNATURE;
    }

    @Override
    public RawIterator<AnyValue[], ProcedureException> apply(
        Context ctx,
        AnyValue[] input,
        ResourceTracker resourceTracker
    ) throws ProcedureException {
        long timeoutInSeconds = ((NumberValue) input[0]).longValue();
        var config = InternalProceduresUtil.resolve(ctx, Config.class);
        var exportPath = config.get(GraphStoreExportSettings.export_location_setting);
        if (exportPath == null) {
            throw new ProcedureException(
                Status.Procedure.ProcedureCallFailed,
                "The configuration '%s' needs to be set in order to use '%s'.",
                GraphStoreExportSettings.export_location_setting.name(),
                PROCEDURE_NAME
            );
        }

        var result = shutdown(
            exportPath,
            InternalProceduresUtil.lookup(ctx, Log.class),
            timeoutInSeconds,
            InternalProceduresUtil.lookup(ctx, AllocationTracker.class)
        );
        return asRawIterator(Stream.ofNullable(new AnyValue[]{Values.booleanValue(result)}));
    }

    private static boolean shutdown(
        Path exportPath,
        Log log,
        long timeoutInSeconds,
        AllocationTracker allocationTracker
    ) {
        var result = BackupAndRestore.backup(
            exportPath,
            log,
            timeoutInSeconds,
            store -> {
                var catalogRequest = CatalogRequest.of(store.userName(), store.graphStore().databaseId());
                GraphStoreCatalog.remove(catalogRequest, store.config().graphName(), graph -> {}, false);
            },
            model -> ModelCatalog.drop(model.creator(), model.name(), false),
            allocationTracker,
            "Shutdown"
        );

        return result.stream().allMatch(BackupAndRestore.BackupResult::done);
    }
}
