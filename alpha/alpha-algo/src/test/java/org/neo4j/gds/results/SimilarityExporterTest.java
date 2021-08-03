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
package org.neo4j.gds.results;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseTest;
import org.neo4j.gds.TestSupport;
import org.neo4j.graphalgo.core.utils.TerminationFlag;

import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

class SimilarityExporterTest extends BaseTest {

    private static final String WRITE_RELATIONSHIP_TYPE = "SIMILARITY";
    private static final String WRITE_PROPERTY = "score";
    private static final String DB_CYPHER = "CREATE (), ()";

    @BeforeEach
    void setUp() {
        runQuery(DB_CYPHER);
    }

    @Test
    void shouldNotDivideByZero() {
        // a list with a single similarity result to export
        final var similarityScore = 1.0;
        var similarityResults = List.of(new SimilarityResult(0, 1, 1, 1, 1, similarityScore, false, false));
        SimilarityExporter similarityExporter = new SimilarityExporter(
            TestSupport.fullAccessTransaction(db),
            WRITE_RELATIONSHIP_TYPE,
            WRITE_PROPERTY,
            TerminationFlag.RUNNING_TRUE
        );
        // should not cause division by zero
        assertThatNoException().isThrownBy(
            () -> similarityExporter.export(similarityResults.stream(), 100)
        );
        // and the result should be written properly
        try (var tx = db.beginTx()) {
            var resultsWritten = tx.getAllRelationships().stream().map(rel -> rel.getProperty("score")).collect(Collectors.toList());
            assertThat(resultsWritten).containsExactly(similarityScore);
        }
    }

}
