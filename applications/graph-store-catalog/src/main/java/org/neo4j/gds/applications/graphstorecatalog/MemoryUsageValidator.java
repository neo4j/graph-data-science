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
package org.neo4j.gds.applications.graphstorecatalog;

import org.neo4j.gds.config.BaseConfig;
import org.neo4j.gds.config.JobIdConfig;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.utils.progress.JobId;
import org.neo4j.gds.exceptions.MemoryEstimationNotImplementedException;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.mem.Estimate;
import org.neo4j.gds.mem.MemoryRange;
import org.neo4j.gds.mem.MemoryTracker;
import org.neo4j.gds.mem.MemoryTreeWithDimensions;

import java.util.StringJoiner;
import java.util.function.Function;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class MemoryUsageValidator {

    private final Log log;
    private final boolean useMaxMemoryEstimation;
    private final MemoryTracker memoryTracker;
    private final String username;

    public MemoryUsageValidator(String username,MemoryTracker memoryTracker, boolean useMaxMemoryEstimation, Log log) {
        this.log = log;
        this.useMaxMemoryEstimation = useMaxMemoryEstimation;
        this.memoryTracker = memoryTracker;
        this.username = username;
    }

    public <C extends BaseConfig & JobIdConfig> MemoryRange tryValidateMemoryUsage(
        String taskName,
        C config,
        Function<C, MemoryTreeWithDimensions> runEstimation
    ) {
        try {
            var memoryTreeWithDimensions = runEstimation.apply(config);

            var estimatedMemoryRange = memoryTreeWithDimensions.memoryTree.memoryUsage();
            if (config.sudo()) {
                log.debug("Sudo mode: Won't check for available memory.");
                memoryTracker.track(
                    username,
                    taskName,
                    config.jobId(),
                    useMaxMemoryEstimation ? estimatedMemoryRange.max : estimatedMemoryRange.min
                );
            } else {
                validateMemoryUsage(
                    username,
                    estimatedMemoryRange,
                    memoryTracker.availableMemory(),
                    useMaxMemoryEstimation,
                    config.jobId(),
                    log
                );
            }

            return estimatedMemoryRange;
        } catch (MemoryEstimationNotImplementedException ignored) {
            return MemoryRange.empty();
        }
    }

    void validateMemoryUsage(
        String taskName,
        MemoryRange estimatedMemoryRange,
        long availableBytes,
        boolean useMaxMemoryEstimation,
        JobId jobId,
        Log log
    ) {
        if (useMaxMemoryEstimation) {
            validateMemoryUsage(
                taskName,
                availableBytes,
                estimatedMemoryRange.max,
                "maximum",
                log, jobId,
                "Consider resizing your Aura instance via console.neo4j.io.",
                "Alternatively, use 'sudo: true' to override the memory validation.",
                "Overriding the validation is at your own risk.",
                "The database can run out of memory and data can be lost."
            );
        } else {
            validateMemoryUsage(
                taskName,
                availableBytes,
                estimatedMemoryRange.min,
                "minimum",
                log, jobId
            );
        }
    }

    private void validateMemoryUsage(
        String taskName,
        long availableBytes,
        long requiredBytes,
        String memoryString,
        Log log, JobId jobId,
        String... messages
    ) {
        if (requiredBytes > availableBytes) {
            var errorMessage = new StringJoiner(" ", "", "");

            errorMessage.add(formatWithLocale(
                "Procedure was blocked since %s estimated memory (%s) exceeds current free memory (%s).",
                memoryString,
                Estimate.humanReadable(requiredBytes),
                Estimate.humanReadable(availableBytes)
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
        memoryTracker.track(username,taskName,jobId, requiredBytes);
    }
}
