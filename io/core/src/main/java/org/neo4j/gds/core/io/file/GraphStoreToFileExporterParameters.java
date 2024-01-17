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
package org.neo4j.gds.core.io.file;

public final class GraphStoreToFileExporterParameters {
    public static GraphStoreToFileExporterParameters create(
        String exportName,
        String username,
        boolean includeMetaData,
        boolean useLabelMapping
    ) {
        return new GraphStoreToFileExporterParameters(exportName, username, includeMetaData, useLabelMapping);
    }

    private final String exportName;
    private final String username;
    private final boolean includeMetaData;
    private final boolean useLabelMapping;

    private GraphStoreToFileExporterParameters(
        String exportName,
        String username,
        boolean includeMetaData,
        boolean useLabelMapping
    ) {
        this.exportName = exportName;
        this.username = username;
        this.includeMetaData = includeMetaData;
        this.useLabelMapping = useLabelMapping;
    }

    public String exportName() {
        return exportName;
    }

    String username() {
        return username;
    }

    boolean includeMetaData() {
        return includeMetaData;
    }

    public boolean useLabelMapping() {
        return useLabelMapping;
    }
}