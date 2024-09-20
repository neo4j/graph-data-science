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
package org.neo4j.gds.compat;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class Neo4jProxyTest {

    @Test
    void shouldLoadProxySuccessfully() {
        // Any access to the proxy will trigger loading an implementation
        assertThatCode(Neo4jProxy::emptyCollector).doesNotThrowAnyException();
    }

    @ParameterizedTest
    @MethodSource("versions")
    void testVersionLongToString(long version, String expected) {
        var proxy = Neo4jProxy.versionLongToString(version);
        assertThat(proxy).isEqualTo(expected);
    }

    static Stream<Arguments> versions() {
        return Stream.of(
            arguments(-1L, "Unknown"),
            arguments(0x0000007473755204L, "Rust"),
            arguments(0x0000000000736902L, "is"),
            arguments(0x0000000065687403L, "the"),
            arguments(0x2165727574754607L, "Future!")
        );
    }
}
