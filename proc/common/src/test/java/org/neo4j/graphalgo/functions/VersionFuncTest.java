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
package org.neo4j.graphalgo.functions;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.TestDatabaseCreator;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.graphalgo.compat.MapUtil.map;

class VersionFuncTest extends BaseProcTest {

    @BeforeEach
    void setup() throws Exception {
        db = TestDatabaseCreator.createTestDatabase();
        registerFunctions(VersionFunc.class);
    }

    @AfterEach
    void tearDown() {
        db.shutdown();
    }

    @Test
    void shouldReturnGradleVersion() throws IOException {
        // we find the current version in the gradle file
        File file = new File("../../gradle/version.gradle");
        Optional<String> maybeVersion = findVersion(file);

        if (!maybeVersion.isPresent()) {
            fail("Could not find version in file: " + file.getAbsolutePath());
        }
        assertCypherResult(
                "RETURN gds.version() AS v",
                singletonList(map("v", maybeVersion.get())));
    }

    private Optional<String> findVersion(File file) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(file));
        Pattern pattern = Pattern.compile(".*gdsVersion = '(\\d\\.\\d\\.\\d+(-alpha\\d+|-beta\\d+)?)'.*");

        String version = null;
        while (reader.ready()) {
            String line = reader.readLine();
            Matcher matcher = pattern.matcher(line);
            if (matcher.matches()) {
                version = matcher.group(1);
            }
        }
        return Optional.ofNullable(version);
    }
}
