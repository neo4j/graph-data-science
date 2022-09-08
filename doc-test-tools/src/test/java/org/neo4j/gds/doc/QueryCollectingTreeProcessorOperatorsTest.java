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

class QueryCollectingTreeProcessorOperatorsTest {

    private QueryCollectingTreeProcessor processor;

    private final Asciidoctor asciidoctor = create();

    @BeforeEach
    void setUp() {
        processor = new QueryCollectingTreeProcessor();
        asciidoctor.javaExtensionRegistry().treeprocessor(processor);

        var file = ResourceUtil.path("query-collecting-tree-processor-test_operators.adoc").toFile();
        assertThat(file).exists().canRead();

        asciidoctor.loadFile(file, Collections.emptyMap());
    }

    @Test
    void loadsBeforeAllQueriesCorrectly() {
        var beforeAllQueries = processor.getBeforeAllQueries();
        assertThat(beforeAllQueries).containsExactly(
            DocQuery.builder().query("CALL my.other.dummy()").operator("ops").build()
        );
    }

    @Test
    void loadsBeforeEachQueriesCorrectly() {
        var beforeEachQueries = processor.getBeforeEachQueries();

        assertThat(beforeEachQueries).containsExactly(
            DocQuery.builder().query("CALL my.other.other.dummy()").operator("ux").build()
        );
    }

    @Test
    void loadsQueryExamplesCorrectly() {
        var queryExampleGroups = processor.getQueryExampleGroups();

        assertThat(queryExampleGroups)
            .containsExactlyInAnyOrder(
                QueryExampleGroup.builder()
                    .displayName("This is a test code block").addQueryExample(
                        QueryExample
                            .builder()
                            .query("CALL my.dummy.stream()")
                            .resultColumns(List.of("Col1"))
                            .addResult(List.of("\"Alice\""))
                            .assertResults(true)
                            .operator("gdsteam").build())
                    .build());
    }


}
