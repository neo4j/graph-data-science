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
package org.neo4j.gds.executor;

import org.neo4j.configuration.Config;
import org.neo4j.gds.compat.GraphDatabaseApiProxy;
import org.neo4j.gds.config.BaseConfig;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.utils.mem.GcListenerExtension;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.core.utils.mem.MemoryTreeWithDimensions;
import org.neo4j.gds.exceptions.MemoryEstimationNotImplementedException;
import org.neo4j.gds.internal.MemoryEstimationSettings;
import org.neo4j.gds.mem.MemoryUsage;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.logging.Log;

import java.util.StringJoiner;
import java.util.function.Function;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class MemoryUsageValidator {

    @FunctionalInterface
    public interface FreeMemoryInspector {
        long freeMemory();
    }

    private final Log log;
    private final GraphDatabaseService api;

    public MemoryUsageValidator(Log log, GraphDatabaseService api) {
        this.log = log;
        this.api = api;
    }

    public <C extends BaseConfig> MemoryRange tryValidateMemoryUsage(C config, Function<C, MemoryTreeWithDimensions> runEstimation) {
        return tryValidateMemoryUsage(config, runEstimation, GcListenerExtension::freeMemory);
    }

    public <C extends BaseConfig> MemoryRange tryValidateMemoryUsage(
        C config,
        Function<C, MemoryTreeWithDimensions> runEstimation,
        FreeMemoryInspector inspector
    ) {
        MemoryTreeWithDimensions memoryTreeWithDimensions = null;

        try {
            memoryTreeWithDimensions = runEstimation.apply(config);
        } catch (MemoryEstimationNotImplementedException ignored) {
        }

        if (memoryTreeWithDimensions == null) {
            return MemoryRange.empty();
        }

        if (config.sudo()) {
            log.debug("Sudo mode: Won't check for available memory.");
        } else {
            var neo4jConfig = GraphDatabaseApiProxy.resolveDependency(api, Config.class);
            var useMaxMemoryEstimation = neo4jConfig.get(MemoryEstimationSettings.validate_using_max_memory_estimation);
            validateMemoryUsage(memoryTreeWithDimensions, inspector.freeMemory(), useMaxMemoryEstimation, log);
        }

        return memoryTreeWithDimensions.memoryTree.memoryUsage();
    }

    static void validateMemoryUsage(
        MemoryTreeWithDimensions memoryTreeWithDimensions,
        long availableBytes,
        boolean useMaxMemoryEstimation,
        Log log
    ) {
        if (useMaxMemoryEstimation) {
            validateMemoryUsage(
                availableBytes,
                memoryTreeWithDimensions.memoryTree.memoryUsage().max,
                "maximum",
                log,
                "Consider resizing your Aura instance via console.neo4j.io.",
                "Alternatively, use 'sudo: true' to override the memory validation.",
                "Overriding the validation is at your own risk.",
                "The database can run out of memory and data can be lost."
            );
        } else {
            validateMemoryUsage(
                availableBytes,
                memoryTreeWithDimensions.memoryTree.memoryUsage().min,
                "minimum",
                log
            );
        }
    }

    private static void validateMemoryUsage(
        long availableBytes,
        long requiredBytes,
        String memoryString,
        Log log,
        String... messages
    ) {
        if (requiredBytes > availableBytes) {
            var errorMessage = new StringJoiner(" ", "", "");

            errorMessage.add(formatWithLocale(
                "Procedure was blocked since %s estimated memory (%s) exceeds current free memory (%s).",
                memoryString,
                MemoryUsage.humanReadable(requiredBytes),
                MemoryUsage.humanReadable(availableBytes)
            ));

            if (!GraphStoreCatalog.isEmpty()) {
                errorMessage.add(formatWithLocale(
                    "Note: there are %s graphs currently loaded into memory.",
                    GraphStoreCatalog.graphStoreCount()
                ));
            }

            for (String message : messages) {
                errorMessage.add(message);
            }

            var message = errorMessage.toString();
            log.info(message);
            throw new IllegalStateException(message);
        }
    }
}
