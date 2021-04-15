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
package org.neo4j.graphalgo.core.utils.export.file.csv;

import org.neo4j.graphalgo.core.utils.export.file.SingleRowVisitor;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class AutoloadFlagVisitor implements SingleRowVisitor<Void> {

    private static final String AUTOLOAD_FILE_NAME = ".autoload";

    private final Path autoloadFlagPath;

    public AutoloadFlagVisitor(Path fileLocation) {
        autoloadFlagPath = fileLocation.resolve(AUTOLOAD_FILE_NAME);
    }

    @Override
    public void export(Void unused) {
        try {
            Files.createFile(autoloadFlagPath);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void close() {

    }
}
