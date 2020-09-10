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
package org.neo4j.graphalgo;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.Result;

import java.io.IOException;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;

class DebugProcTest extends BaseProcTest {

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(DebugProc.class);
    }

    @Test
    void runDebug() throws IOException {
        var result = runQuery("CALL gds.debug()", Result::resultAsString);
        System.out.println(result);
    }

    @Test
    void shouldReturnGradleVersion() throws IOException {
        var result = runQuery(
            "CALL gds.debug() YIELD key, value WITH key, value WHERE key = 'gdsVersion' RETURN value as gdsVersion",
            cypherResult -> cypherResult.<String>columnAs("gdsVersion").stream().collect(toList())
        );
        // TODO: load this from properties as well
        assertThat(result).containsExactly("1.4.0-alpha04");
    }
}
