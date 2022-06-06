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
package org.neo4j.gds.doc;

import org.asciidoctor.Asciidoctor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;

import static org.asciidoctor.Asciidoctor.Factory.create;
import static org.assertj.core.api.Assertions.assertThat;

class QueryCollectingTreeProcessorTest {

    private QueryCollectingTreeProcessor processor;

    @BeforeEach
    void setUp() throws URISyntaxException {
        Asciidoctor asciidoctor = create();
        processor = new QueryCollectingTreeProcessor();
        asciidoctor.javaExtensionRegistry().treeprocessor(processor);

        var file = Paths
            .get(getClass().getClassLoader().getResource("query-collecting-tree-processor-test.adoc").toURI())
            .toFile();
        assertThat(file).exists().canRead();

        asciidoctor.loadFile(file, Collections.emptyMap());
    }

    @Test
    void loadsBeforeAllQueriesCorrectly() {
        var beforeAllQueries = processor.beforeAllQueries();
        assertThat(beforeAllQueries).containsExactly(
            "CREATE (alice:Person {name: 'Alice'})",
            "CREATE (bob:Person {name: 'Bob'})"
        );
    }

    @Test
    void loadsBeforeEachQueriesCorrectly() {
        var beforeEachQueries = processor.beforeEachQueries();

        assertThat(beforeEachQueries).containsExactly(
            "MATCH (n) RETURN n",
            "MATCH (m) RETURN m",
            "MATCH (q) RETURN q"
        );
    }

    @Test
    void loadsQueryExamplesCorrectly() {
        var queryExampleGroups = processor.queryExamples();

        assertThat(queryExampleGroups)
            .containsExactlyInAnyOrder(
                QueryExampleGroup.builder()
                    .displayName("This is a test code block")
                    .addQueryExample(
                        QueryExample
                            .builder()
                            .query("CALL gds.nodeSimilarity.stream() YIELD node1, node2, similarity")
                            .resultColumns(List.of("Person1", "Person2", "similarity"))
                            .addResult(List.of("\"Alice\"", "\"Dave\"", "1.0"))
                            .addResult(List.of("\"Dave\"", "\"Alice\"", "1.0"))
                            .assertResults(true)
                            .build())
                    .build(),
                QueryExampleGroup.builder()
                    .displayName("testGroup")
                    .addQueryExample(
                        QueryExample
                            .builder()
                            .query("CALL gds.nodeSimilarity.stream() YIELD node1, node2, similarity")
                            .resultColumns(List.of("Person1", "Person2", "similarity"))
                            .addResult(List.of("\"Alice\"", "\"Dave\"", "1.0"))
                            .addResult(List.of("\"Dave\"", "\"Alice\"", "1.0"))
                            .assertResults(true)
                            .build())
                    .addQueryExample(
                        QueryExample
                            .builder()
                            .query("CALL gds.nodeSimilarity.stream() YIELD node1, node2, similarity")
                            .resultColumns(List.of("Person1", "Person2", "similarity"))
                            .addResult(List.of("\"Alice\"", "\"Dave\"", "1.0"))
                            .addResult(List.of("\"Dave\"", "\"Alice\"", "1.0"))
                            .assertResults(true)
                            .build())
                    .build(),
                QueryExampleGroup.builder()
                    .displayName("This is a no-results test code block")
                    .addQueryExample(
                        QueryExample
                            .builder()
                            .query("CALL gds.nodeSimilarity.stream() YIELD node1, node2, similarity")
                            .resultColumns(List.of())
                            .results(List.of())
                            .assertResults(false)
                            .build())
                    .build()
            );
    }
}
