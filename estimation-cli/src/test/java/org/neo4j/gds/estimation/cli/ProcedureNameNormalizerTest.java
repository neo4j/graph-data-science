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
package org.neo4j.gds.estimation.cli;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ProcedureNameNormalizerTest {

    private static String run(String input) {
        return new ProcedureNameNormalizer().convert(input);
    }

    @Test
    void convertNormalizedToLowercase() {
        assertEquals("gds.foo.bar.estimate", run("gDs.FoO.bAR.eStImAtE"));
    }

    @Test
    void convertAppendsEstimate() {
        assertEquals("gds.foo.estimate", run("gds.foo"));
        assertEquals("gds.foo.estimate", run("gds.foo.estimate"));
        assertEquals("gds.foo-estimate.estimate", run("gds.foo-estimate"));
        assertEquals("gds.foo.estimate.bar.estimate", run("gds.foo.estimate.bar"));
    }

    @Test
    void convertPrependsGds() {
        assertEquals("gds.foo.estimate", run("foo.estimate"));
        assertEquals("gds.foo.estimate", run("gds.foo.estimate"));
        assertEquals("gds.gds-foo.estimate", run("gds-foo.estimate"));
        assertEquals("gds.foo.gds.bar.estimate", run("foo.gds.bar.estimate"));
    }
}
