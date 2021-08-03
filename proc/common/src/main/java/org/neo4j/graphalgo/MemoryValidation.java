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

import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;
import org.neo4j.graphalgo.core.utils.mem.MemoryTreeWithDimensions;
import org.neo4j.graphalgo.core.utils.mem.MemoryUsage;

import java.util.StringJoiner;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

final class MemoryValidation {

    static void validateMemoryUsage(
        MemoryTreeWithDimensions memoryTreeWithDimensions,
        long availableBytes,
        boolean useMaxMemoryEstimation
    ) {
        if (useMaxMemoryEstimation) {
            validateMemoryUsage(
                availableBytes,
                memoryTreeWithDimensions.memoryTree.memoryUsage().max,
                "maximum",
                "Consider resizing your Aura instance via console.neo4j.io.",
                "Alternatively, use 'sudo: true' to override the memory validation.",
                "Overriding the validation is at your own risk.",
                "The database can run out of memory and data can be lost."
            );
        } else {
            validateMemoryUsage(
                availableBytes,
                memoryTreeWithDimensions.memoryTree.memoryUsage().min,
                "minimum"
            );
        }
    }

    private static void validateMemoryUsage(
        long availableBytes,
        long requiredBytes,
        String memoryString,
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
                    GraphStoreCatalog.graphStoresCount()
                ));
            }

            for (String message : messages) {
                errorMessage.add(message);
            }

            throw new IllegalStateException(errorMessage.toString());
        }
    }

    private MemoryValidation() {}
}
