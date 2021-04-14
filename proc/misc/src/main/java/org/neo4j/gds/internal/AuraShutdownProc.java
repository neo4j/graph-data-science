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

import org.neo4j.configuration.Config;
import org.neo4j.graphalgo.BaseProc;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.compat.GraphDatabaseApiProxy;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.export.file.GraphStoreExporterUtil;
import org.neo4j.graphalgo.core.utils.export.file.ImmutableGraphStoreToFileExporterConfig;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Internal;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;
import static org.neo4j.procedure.Mode.READ;

public class AuraShutdownProc extends BaseProc {

    @Internal
    @Procedure(name = "gds.internal.shutdown", mode = READ)
    @Description("Persists graph stores to disk.")
    public Stream<ShutdownResult> persist(
        @Name(value = "timeoutInSeconds", defaultValue = "42") long timeoutInSeconds
    ) {
        var timer = ProgressTimer.start();
        try (timer) {
            var neo4jConfig = GraphDatabaseApiProxy.resolveDependency(api, Config.class);
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
                            allocationTracker()
                        );
                        return Stream.empty();
                    } catch (Exception e) {
                        return Stream.of(ImmutableFailedExport.of(e, store.userName(), createConfig.graphName()));
                    }
                })
                .collect(Collectors.toList());

            if (!failedExports.isEmpty()) {
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
                return Stream.of(new ShutdownResult(false));
            }
        }

        var took = timer.getDuration();
        if (TimeUnit.MILLISECONDS.toSeconds(took) > timeoutInSeconds) {
            log.warn(
                "Shutdown took too long, the actual time of %d seconds is greater than the provided timeout of %d seconds",
                TimeUnit.MILLISECONDS.toSeconds(took),
                timeoutInSeconds
            );
        } else {
            log.info(
                "Shutdown happened within the given timeout, it took %d seconds and the provided timeout as %d seconds.",
                TimeUnit.MILLISECONDS.toSeconds(took),
                timeoutInSeconds
            );
        }
        return Stream.of(new ShutdownResult(true));
    }

    public static final class ShutdownResult {
        public final boolean done;

        ShutdownResult(boolean done) {
            this.done = done;
        }
    }

    @ValueClass
    interface FailedExport {
        Exception exception();

        String userName();

        String graphName();
    }
}
