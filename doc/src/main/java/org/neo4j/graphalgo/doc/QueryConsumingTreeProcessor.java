/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
package org.neo4j.graphalgo.doc;

import org.asciidoctor.ast.Cell;
import org.asciidoctor.ast.Document;
import org.asciidoctor.ast.Row;
import org.asciidoctor.ast.StructuralNode;
import org.asciidoctor.ast.Table;
import org.asciidoctor.extension.Treeprocessor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Extension for Asciidoctor.
 * It looks for two kinds of blocks in the AsciiDoc source:
 * 1) Blocks with the role `setup-query`. The content is passed to a SetupQueryConsumer.
 *    A doc test can use this by making the SetupQueryConsumer run the content as a setup query before running tests.
 * 2) Blocks with the role `query-example`. The content is expected to contain exactly two blocks:
 * 2.1) First, a block containing a Cypher query example.
 * 2.2) Second, a table containing a Cypher query result.
 *    The query, the table header, and all the rows, are passed to the QueryExampleConsumer.
 *    A doc test can use this by making the QueryExampleConsumer run the queries and assert that the result on each row matches the result
 */
public class QueryConsumingTreeProcessor extends Treeprocessor {

    private static final String SETUP_QUERY_ROLE = "setup-query";
    private static final String QUERY_EXAMPLE_ROLE = "query-example";
    private final SetupQueryConsumer setupQueryConsumer;
    private final QueryExampleConsumer queryExampleConsumer;

    public QueryConsumingTreeProcessor(
        SetupQueryConsumer setupQueryConsumer,
        QueryExampleConsumer queryExampleConsumer
    ) {
        this.setupQueryConsumer = setupQueryConsumer;
        this.queryExampleConsumer = queryExampleConsumer;
    }

    @Override
    public Document process(Document document) {
        consumeSetupQueries(document);
        consumeQueryExamples(document);
        return document;
    }

    private void consumeSetupQueries(Document document) {
        List<String> setupQueries = new ArrayList<>();
        collectSetupQueries(document, setupQueries);
        setupQueryConsumer.consume(setupQueries);
    }

    private void collectSetupQueries(StructuralNode node, List<String> setupQueries) {
        node.getBlocks().stream()
            .filter(block -> block instanceof StructuralNode)
            .forEach(it -> {
                if (it.getRoles().contains(SETUP_QUERY_ROLE)) {
                    setupQueries.add(undoReplacements(it.getContent().toString()));
                } else {
                    collectSetupQueries(it, setupQueries);
                }
            });
    }

    private void consumeQueryExamples(StructuralNode node) {
        node.getBlocks()
            .forEach(it -> {
                if (it.getRoles().contains(QUERY_EXAMPLE_ROLE)) {
                    processCypherExample(it);
                } else {
                    consumeQueryExamples(it);
                }
            });
    }

    private void processCypherExample(StructuralNode cypherExample) {
        List<StructuralNode> blocks = cypherExample.getBlocks();
        StructuralNode queryBlock = blocks.get(0);
        Table resultTable = (Table) blocks.get(1);

        String query = queryBlock.getContent().toString();
        List<String> headers = headers(resultTable);
        List<Row> rows = resultTable.getBody();

        queryExampleConsumer.consume(query, headers, rows);
    }

    private List<String> headers(Table table) {
        return table.getHeader().get(0).getCells().stream().map(Cell::getText).collect(Collectors.toList());
    }

    private String undoReplacements(String content) {
        return content
            .replaceAll("&gt;", ">")
            .replaceAll("&lt;", "<");
    }

    static class Testable {

        private final List<String> columns;
        private final List<Row> rows;

        public Testable(List<String> columns, List<Row> rows) {
            this.columns = columns;
            this.rows = rows;
        }

        static Testable of(List<String> headers, List<Row> rows) {
            return new Testable(headers, rows);
        }

        List<String> columns() {
            return columns;
        }

        List<Row> rows() {
            return rows;
        }

        List<Map<String, Object>> toMap() {
            return rows().stream().map(row -> {
                Map<String, Object> thing = new HashMap<>();
                IntStream.range(0, row.getCells().size())
                    .forEach(i -> thing.put(columns().get(i), row.getCells().get(i).getText()));
                return thing;
            }).collect(Collectors.toList());
        }
    }

    @FunctionalInterface
    interface QueryExampleConsumer {
        void consume(String query, List<String> columns, List<Row> rows);
    }

    @FunctionalInterface
    interface SetupQueryConsumer {
        void consume(List<String> setupQueries);
    }

}
