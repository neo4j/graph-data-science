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
package org.neo4j.gds.algorithms;

import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.core.utils.mem.GcListenerExtension;
import org.neo4j.gds.core.utils.mem.MemoryTreeWithDimensions;
import org.neo4j.gds.exceptions.MemoryEstimationNotImplementedException;
import org.neo4j.gds.mem.Estimate;
import org.neo4j.gds.logging.Log;

import java.util.StringJoiner;
import java.util.function.Function;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

/*
A modified copy of `org.neo4j.gds.executor.MemoryUsageValidator` so we don't introduce circular dependency to `:executor`...
 */
public class AlgorithmMemoryValidationService {

    @FunctionalInterface
    public interface FreeMemoryInspector {
        long freeMemory();
    }

    private final Log log;
    private final boolean useMaxMemoryEstimation;

    public AlgorithmMemoryValidationService(Log log, boolean useMaxMemoryEstimation) {
        this.log = log;
        this.useMaxMemoryEstimation = useMaxMemoryEstimation;
    }

    public <C extends AlgoBaseConfig> void validateAlgorithmCanRunWithTheAvailableMemory(
        C config,
        Function<C, MemoryTreeWithDimensions> runEstimation,
        long graphStoreCount
    ) throws IllegalStateException {
        validateAlgorithmCanRunWithTheAvailableMemory(config, runEstimation, GcListenerExtension::freeMemory, graphStoreCount);
    }

    private <C extends AlgoBaseConfig> void validateAlgorithmCanRunWithTheAvailableMemory(
        C config,
        Function<C, MemoryTreeWithDimensions> runEstimation,
        FreeMemoryInspector inspector,
        long graphStoreCount
    ) throws IllegalStateException {
        if (config.sudo()) {
            log.debug("Sudo mode: Won't check for available memory.");
        } else {
            MemoryTreeWithDimensions memoryTreeWithDimensions;

            try {
                memoryTreeWithDimensions = runEstimation.apply(config);

                validateMemoryUsage(
                    memoryTreeWithDimensions,
                    inspector.freeMemory(),
                    useMaxMemoryEstimation,
                    log,
                    graphStoreCount
                );
            } catch (MemoryEstimationNotImplementedException ignored) {
                // not all algorithms have memory estimation implementation.
            }
        }
    }

    private static void validateMemoryUsage(
        MemoryTreeWithDimensions memoryTreeWithDimensions,
        long availableBytes,
        boolean useMaxMemoryEstimation,
        Log log,
        long graphStoreCount
    ) throws IllegalStateException {
        if (useMaxMemoryEstimation) {
            validateMemoryUsage(
                availableBytes,
                memoryTreeWithDimensions.memoryTree.memoryUsage().max,
                "maximum",
                log,
                graphStoreCount,
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
                log,
                graphStoreCount
            );
        }
    }

    private static void validateMemoryUsage(
        long availableBytes,
        long requiredBytes,
        String memoryString,
        Log log,
        long graphStoresCount,
        String... messages
    ) throws IllegalStateException {
        if (requiredBytes > availableBytes) {
            var errorMessage = new StringJoiner(" ", "", "");

            errorMessage.add(formatWithLocale(
                "Procedure was blocked since %s estimated memory (%s) exceeds current free memory (%s).",
                memoryString,
                Estimate.humanReadable(requiredBytes),
                Estimate.humanReadable(availableBytes)
            ));

            if (graphStoresCount > 0) {
                errorMessage.add(formatWithLocale(
                    "Note: there are %s graphs currently loaded into memory.",
                    graphStoresCount
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
