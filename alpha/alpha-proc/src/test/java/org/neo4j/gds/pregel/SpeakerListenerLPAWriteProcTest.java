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
package org.neo4j.gds.pregel;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.beta.generator.GraphGenerateProc;
import org.neo4j.graphdb.QueryExecutionException;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

class SpeakerListenerLPAWriteProcTest extends BaseProcTest {

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            SpeakerListenerLPAWriteProc.class,
            GraphGenerateProc.class
        );
    }

    @Test
    void shouldFailWhenRunningOnNonWritableGraph() {
        runQuery("CALL gds.beta.graph.generate('randomGraph', 5, 2, {relationshipSeed:19}) YIELD name, nodes, relationships, relationshipDistribution");

        assertThatExceptionOfType(QueryExecutionException.class)
            .isThrownBy(
                () -> runQuery("CALL gds.alpha.sllpa.write('randomGraph', {writeProperty: 'm', maxIterations: 4, minAssociationStrength: 0.1})")
            )
            .withRootCauseInstanceOf(IllegalArgumentException.class)
            .withMessageContaining("The provided graph does not support `write` execution mode.");
    }
}
