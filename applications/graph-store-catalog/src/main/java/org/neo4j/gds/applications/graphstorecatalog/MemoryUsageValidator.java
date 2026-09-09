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

import org.neo4j.gds.api.User;
import org.neo4j.gds.config.BaseConfig;
import org.neo4j.gds.config.JobIdConfig;
import org.neo4j.gds.core.JobId;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.exceptions.MemoryEstimationNotImplementedException;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.mem.Estimate;
import org.neo4j.gds.mem.MemoryRange;
import org.neo4j.gds.mem.MemoryTreeWithDimensions;
import org.neo4j.gds.memory.tracking.MemoryTracker;

import java.util.StringJoiner;
import java.util.function.Function;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class MemoryUsageValidator {
    private final Log log;
    private final MemoryTracker memoryTracker;
    private final User user;
    private final boolean useMaxMemoryEstimation;

    public MemoryUsageValidator(
        Log log,
        User user,
        MemoryTracker memoryTracker,
        boolean useMaxMemoryEstimation
    ) {
        this.log = log;
        this.user = user;
        this.memoryTracker = memoryTracker;
        this.useMaxMemoryEstimation = useMaxMemoryEstimation;
    }

    public synchronized <C extends BaseConfig & JobIdConfig> MemoryRange tryValidateMemoryUsage(
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
                    user.getUsername(),
                    taskName,
                    config.jobId(),
                    useMaxMemoryEstimation ? estimatedMemoryRange.max : estimatedMemoryRange.min
                );
            } else {
                validateMemoryUsage(
                    taskName,
                    memoryTracker.availableMemory(),
                    estimatedMemoryRange,
                    config.jobId()
                );
            }

            return estimatedMemoryRange;
        } catch (MemoryEstimationNotImplementedException ignored) {
            return MemoryRange.empty();
        }
    }

    void validateMemoryUsage(
        String taskName,
        long availableBytes,
        MemoryRange estimatedMemoryRange,
        JobId jobId
    ) {
        if (useMaxMemoryEstimation) {
            _validateMemoryUsage(
                taskName,
                availableBytes,
                estimatedMemoryRange.max,
                "maximum",
                jobId,
                "Consider resizing your Aura instance via console.neo4j.io.",
                "Alternatively, use 'sudo: true' to override the memory validation.",
                "Overriding the validation is at your own risk.",
                "The database can run out of memory and data can be lost."
            );
        } else {
            _validateMemoryUsage(
                taskName,
                availableBytes,
                estimatedMemoryRange.min,
                "minimum",
                jobId
            );
        }
    }

    private void _validateMemoryUsage(
        String taskName,
        long availableBytes,
        long requiredBytes,
        String memoryString,
        JobId jobId,
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
        memoryTracker.track(user.getUsername(), taskName, jobId, requiredBytes);
    }
}
