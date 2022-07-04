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
package org.neo4j.gds.core.io.file.csv;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AutoloadFlagVisitorTest {

    @TempDir
    Path tempDir;

    @Test
    void shouldCreateAutoloadFlagFile() {
        var autoloadFlagVisitor = new AutoloadFlagVisitor(tempDir);

        autoloadFlagVisitor.export();

        assertThat(tempDir).isDirectoryContaining("glob:**/.autoload");
    }

    @Test
    void shouldFailIfAutoloadFlagFileAlreadyExists() throws IOException {
        Files.createFile(tempDir.resolve(".autoload"));

        var autoloadFlagVisitor = new AutoloadFlagVisitor(tempDir);

        assertThatThrownBy(autoloadFlagVisitor::export)
            .hasRootCauseInstanceOf(FileAlreadyExistsException.class);
    }

}
