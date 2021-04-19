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
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.compat.Neo4jProxy;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.export.file.GraphStoreExporterUtil;
import org.neo4j.graphalgo.core.utils.export.file.ImmutableGraphStoreToFileExporterConfig;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.DefaultParameterValue;
import org.neo4j.internal.kernel.api.procs.FieldSignature;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.kernel.api.ResourceTracker;
import org.neo4j.kernel.api.procedure.CallableProcedure;
import org.neo4j.kernel.api.procedure.Context;
import org.neo4j.logging.Log;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.NumberValue;
import org.neo4j.values.storable.Values;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;
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
        var result = shutdown(
            InternalProceduresUtil.resolve(ctx, Config.class),
            InternalProceduresUtil.lookup(ctx, Log.class),
            timeoutInSeconds,
            InternalProceduresUtil.lookup(ctx, AllocationTracker.class)
        );
        return asRawIterator(Stream.ofNullable(new AnyValue[]{Values.booleanValue(result)}));
    }

    private static boolean shutdown(
        Config neo4jConfig,
        Log log,
        long timeoutInSeconds,
        AllocationTracker allocationTracker
    ) {
        var timer = ProgressTimer.start();
        try (timer) {
            var success = exportAllGraphStores(neo4jConfig, log, allocationTracker);
            if(!success) {
                return false;
            }
        }

        var elapsedTimeInSeconds = TimeUnit.MILLISECONDS.toSeconds(timer.getDuration());
        if (elapsedTimeInSeconds > timeoutInSeconds) {
            log.warn(
                "Shutdown took too long, the actual time of %d seconds is greater than the provided timeout of %d seconds",
                elapsedTimeInSeconds,
                timeoutInSeconds
            );
        } else {
            log.info(
                "Shutdown happened within the given timeout, it took %d seconds and the provided timeout as %d seconds.",
                elapsedTimeInSeconds,
                timeoutInSeconds
            );
        }
        return true;
    }

    private static boolean exportAllGraphStores(
        Config neo4jConfig,
        Log log,
        AllocationTracker allocationTracker
    ) {
        var failedExports = GraphStoreCatalog.getAllGraphStores()
            .flatMap(store -> {
                var createConfig = store.config();
                var graphStore = store.graphStore();
                try {
                    var config = ImmutableGraphStoreToFileExporterConfig
                        .builder()
                        .includeMetaData(true)
                        .exportName(createConfig.graphName())
                        .username(store.userName())
                        .build();

                    GraphStoreExporterUtil.runGraphStoreExportToCsv(
                        graphStore,
                        neo4jConfig,
                        config,
                        log,
                        allocationTracker
                    );
                    return Stream.empty();
                } catch (Exception e) {
                    return Stream.of(ImmutableFailedExport.of(e, store.userName(), createConfig.graphName()));
                }
            })
            .collect(Collectors.toList());

        for (var failedExport : failedExports) {
            log.warn(
                formatWithLocale(
                    "GraphStore persistence failed on graph %s for user %s",
                    failedExport.graphName(),
                    failedExport.userName()
                ),
                failedExport.exception()
            );
        }

        return failedExports.isEmpty();
    }

    @ValueClass
    interface FailedExport {
        Exception exception();

        String userName();

        String graphName();
    }
}
