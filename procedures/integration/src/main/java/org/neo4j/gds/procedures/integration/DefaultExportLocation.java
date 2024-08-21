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

import org.neo4j.gds.applications.graphstorecatalog.ExportLocation;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.settings.GdsSettings;
import org.neo4j.graphdb.config.Configuration;

import java.nio.file.Path;
import java.util.Optional;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class DefaultExportLocation implements ExportLocation {
    private final Log log;
    private final Configuration neo4jConfiguration;

    public DefaultExportLocation(Log log, Configuration neo4jConfiguration) {
        this.log = log;
        this.neo4jConfiguration = neo4jConfiguration;
    }

    @Override
    public Path getAcceptingError() {
        var exportLocation = neo4jConfiguration.get(GdsSettings.exportLocation());

        if (exportLocation == null) throw new IllegalStateException(formatWithLocale(
            "The configuration '%s' needs to be set.",
            GdsSettings.exportLocation().name()
        ));

        return exportLocation;
    }

    @Override
    public Optional<Path> getAcceptingNull() {
        var exportLocation = neo4jConfiguration.get(GdsSettings.exportLocation());

        if (exportLocation == null) log.warn(
            "[gds] The configuration %s is missing.",
            GdsSettings.exportLocation().name()
        );

        return Optional.ofNullable(exportLocation);
    }
}
