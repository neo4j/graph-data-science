/*
 * Copyright (c) 2017-2020 "Neo4j,"
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

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.neo4j.graphalgo.core.utils.export.file.ImmutableGraphStoreFileExportConfig;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class GraphStoreFileExportConfigTest {

    @Test
    void exportPathMustBeAbsolute() {
        var ex = assertThrows(IllegalArgumentException.class, () -> {
            ImmutableGraphStoreFileExportConfig
                .builder()
                .exportLocation("_THIS_IS_NOT_ABSOLUTE")
                .build()
                .validateExportLocation();
        });

        assertThat(ex).hasMessage("The parameter `exportLocation` must be an absolute path.");
    }

    @Test
    void exportPathMustExist() {
        var ex = assertThrows(IllegalArgumentException.class, () -> {
            ImmutableGraphStoreFileExportConfig
                .builder()
                .exportLocation("/this/does/not/exist")
                .build()
                .validateExportLocation();
        });

        assertThat(ex).hasMessage(
            "Cannot access the specified export location at /this/does/not/exist. Please make sure it exists and is accessible");
    }

    @Disabled("Apparently we cannot change directory permissions on TeamCity")
    void exportPathMustBeWriteable(@TempDir File tempDir) {
        tempDir.setReadOnly();

        var ex = assertThrows(IllegalArgumentException.class, () -> {
            ImmutableGraphStoreFileExportConfig
                .builder()
                .exportLocation(tempDir.getAbsolutePath())
                .build()
                .validateExportLocation();
        });

        assertThat(ex).hasMessage(
            "Cannot access the specified export location at %s. Please make sure it exists and is accessible",
            tempDir.getAbsoluteFile()
        );
    }

}
