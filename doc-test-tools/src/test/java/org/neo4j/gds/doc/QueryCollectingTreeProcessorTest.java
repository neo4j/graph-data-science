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
import org.neo4j.gds.ResourceUtil;
import org.neo4j.gds.doc.syntax.DocQuery;

import java.util.Collections;
import java.util.List;

import static org.asciidoctor.Asciidoctor.Factory.create;
import static org.assertj.core.api.Assertions.assertThat;

class QueryCollectingTreeProcessorTest {

    private QueryCollectingTreeProcessor processor;

    private final Asciidoctor asciidoctor = create();

    @BeforeEach
    void setUp() {
        processor = new QueryCollectingTreeProcessor();
        asciidoctor.javaExtensionRegistry().treeprocessor(processor);

        var file = ResourceUtil.path("query-collecting-tree-processor-test.adoc").toFile();
        assertThat(file).exists().canRead();

        asciidoctor.loadFile(file, Collections.emptyMap());
    }

    @Test
    void loadsBeforeAllQueriesCorrectly() {
        var beforeAllQueries = processor.beforeAllQueries();
        assertThat(beforeAllQueries).containsExactly(
            DocQuery.builder().query("CREATE (alice:Person {name: 'Alice'})").build(),
            DocQuery.builder().query(
                "CREATE (bob:Person {name: 'Bob'})").build()
        );
    }

    @Test
    void loadsBeforeEachQueriesCorrectly() {
        var beforeEachQueries = processor.beforeEachQueries();

        assertThat(beforeEachQueries).containsExactly(
            DocQuery.builder().query(
                "MATCH (n) RETURN n").build(),
            DocQuery.builder().query(
                "MATCH (m) RETURN m").build(),
            DocQuery.builder().query(
                "MATCH (q) RETURN q").build()
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

    @Test
    void parseMultipleDocument() {
        var file = ResourceUtil.path("query-collecting-tree-processor-test_part2.adoc").toFile();
        assertThat(file).exists().canRead();

        asciidoctor.loadFile(file, Collections.emptyMap());

        assertThat(processor.queryExamples())
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
                    .addQueryExample(
                        QueryExample
                            .builder()
                            .query("CALL my.dummy.stream()")
                            .resultColumns(List.of("Col1"))
                            .addResult(List.of("\"Alice\""))
                            .assertResults(true)
                            .build()
                    )
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
                    .build(),
                QueryExampleGroup.builder()
                    .displayName("groupSecond")
                    .addQueryExample(
                        QueryExample
                            .builder()
                            .query("CALL my.other.dummy()")
                            .assertResults(false)
                            .build())
                    .build()
            );
    }
}
