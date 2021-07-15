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
import org.neo4j.graphalgo.compat.GraphStoreExportSettings;
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

import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;
import static org.neo4j.internal.helpers.collection.Iterators.asRawIterator;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTBoolean;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTInteger;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTString;
import static org.neo4j.procedure.Mode.READ;

public class AuraBackupProc implements CallableProcedure {

    private static final QualifiedName PROCEDURE_NAME = new QualifiedName(
        new String[]{"gds", "internal"},
        "backup"
    );

    private static final ProcedureSignature SIGNATURE = Neo4jProxy.procedureSignature(
        PROCEDURE_NAME,
        // Input signature: [
        //   @Name(value = "timeoutInSeconds", defaultValue = "42") long timeoutInSeconds
        // ]
        List.of(FieldSignature.inputField("timeoutInSeconds", NTInteger, DefaultParameterValue.ntInteger(42))),
        // Output type: return a boolean
        List.of(FieldSignature.outputField("done", NTBoolean), FieldSignature.outputField("backupName", NTString)),
        // Procedure mode
        READ,
        // Do not require admin user for execution
        false,
        // No deprecation
        null,
        // Roles that are explicitly allowed to call this procedure (none)
        new String[0],
        // Procedure description
        "Backup graph and model catalogs.",
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

    private static final String GRAPHS_DIR = "graphs";
    private static final String MODELS_DIR = "models";

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
        var neo4jConfig = InternalProceduresUtil.resolve(ctx, Config.class);
        var backupName = formatWithLocale("backup-%s", UUID.randomUUID());
        var backupRoot = neo4jConfig.get(GraphStoreExportSettings.backup_location_setting).resolve(backupName);
        long timeoutInSeconds = ((NumberValue) input[0]).longValue();

        var result = shutdown(
            backupRoot,
            InternalProceduresUtil.lookup(ctx, Log.class),
            timeoutInSeconds,
            InternalProceduresUtil.lookup(ctx, AllocationTracker.class)
        );
        return asRawIterator(Stream.ofNullable(new AnyValue[]{Values.booleanValue(result), Values.stringValue(backupName)}));
    }

    private static boolean shutdown(
        Path backupRoot,
        Log log,
        long timeoutInSeconds,
        AllocationTracker allocationTracker
    ) {
        log.info("Preparing for backup");
        var timer = ProgressTimer.start();
        try (timer) {
            if(!exportAllGraphStores(backupRoot.resolve(GRAPHS_DIR), log, allocationTracker)) {
                return false;
            }
            if(!exportAllModels(backupRoot.resolve(MODELS_DIR), log, allocationTracker)) {
                return false;
            }
        }

        var elapsedTimeInSeconds = TimeUnit.MILLISECONDS.toSeconds(timer.getDuration());
        if (elapsedTimeInSeconds > timeoutInSeconds) {
            log.warn(
                "Backup took too long, the actual time of %d seconds is greater than the provided timeout of %d seconds",
                elapsedTimeInSeconds,
                timeoutInSeconds
            );
        } else {
            log.info(
                "Backup happened within the given timeout, it took %d seconds and the provided timeout was %d seconds.",
                elapsedTimeInSeconds,
                timeoutInSeconds
            );
        }
        return true;
    }

    private static boolean exportAllGraphStores(
        Path backupRoot,
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
                        .autoload(true)
                        .exportName(createConfig.graphName())
                        .username(store.userName())
                        .build();

                    var backupPath = GraphStoreExporterUtil.getExportPath(backupRoot, config);

                    GraphStoreExporterUtil.runGraphStoreExportToCsv(
                        graphStore,
                        backupPath,
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

    private static boolean exportAllModels(
        Path backupRoot,
        Log log,
        AllocationTracker allocationTracker
    ) {
        return true;
    }

    @ValueClass
    interface FailedExport {
        Exception exception();

        String userName();

        String graphName();
    }
}
