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
package org.neo4j.gds.procedures.integration;

import org.neo4j.gds.settings.GdsSettings;
import org.neo4j.graphdb.config.Configuration;

import java.nio.file.Path;
import java.util.function.Supplier;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class ExportLocation {
    private ExportLocation() {}

    /**
     * Right, when I made this eager, 500 tests failed. So indirection it is.
     */
    public static Supplier<Path> create(Configuration neo4jConfiguration) {
        return () -> {
            var exportLocation = neo4jConfiguration.get(GdsSettings.exportLocation());

            if (exportLocation == null) throw new IllegalStateException(formatWithLocale(
                "The configuration '%s' needs to be set.",
                GdsSettings.exportLocation().name()
            ));

            return exportLocation;
        };
    }
}
