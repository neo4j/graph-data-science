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
package org.neo4j.gds.datasets;

import java.io.IOException;
import java.nio.file.Path;

public abstract class Dataset {

    private final String id;

    public Dataset(String id) {
        this.id = id;
    }

    /**
     * Create here means e.g. generate data in an embedded database, or download a dataset from a remote location.
     *
     * In the end database files end up in the dataset dir, ready for being started using a database.
     */
    public final void prepare(Path datasetDir, DbCreator dbCreator) throws IOException {
        if (isDownloadingKind()) downloadAndInstall(datasetDir); else generate(datasetDir, dbCreator);
    }

    protected boolean isDownloadingKind() {
        return false; // because majority are generators
    }

    protected void downloadAndInstall(Path datasetDir) throws IOException {
        // no-op
    }

    protected void generate(Path datasetDir, DbCreator dbCreator) {
        // no-op
    }

    public String getId() {
        return id;
    }
}
