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

import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class UserInfoLoaderTest {

    @ParameterizedTest
    @ValueSource(strings = {
        "UserA",
        "UserA     ",
        "UserA\n",
        "UserA\t\n"
    })
    void shouldReadUsername(CharSequence fileContent, @TempDir Path tempDir) throws IOException {
        Files.writeString(tempDir.resolve(UserInfoVisitor.USER_INFO_FILE_NAME), fileContent, StandardCharsets.UTF_8);

        var username = new UserInfoLoader(tempDir).load();

        assertThat(username).isEqualTo("UserA");
    }

}
