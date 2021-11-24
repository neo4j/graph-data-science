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

import org.asciidoctor.ast.Cell;
import org.asciidoctor.ast.ContentNode;
import org.asciidoctor.ast.Document;
import org.asciidoctor.ast.Row;
import org.asciidoctor.ast.StructuralNode;
import org.asciidoctor.ast.Table;
import org.asciidoctor.extension.Treeprocessor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

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

    private static final String CODE_BLOCK_CONTEXT = ":listing";
    private static final String TABLE_CONTEXT = ":table";

    private static final String SETUP_QUERY_ROLE = "setup-query";
    private static final String GRAPH_CREATE_QUERY_ROLE = "graph-create-query";
    private static final String QUERY_EXAMPLE_ROLE = "query-example";
    private static final String TEST_TYPE_NO_RESULT = "no-result";
    private static final String TEST_GROUP_ATTRIBUTE = "group";
    private static final String ROLE_SELECTOR = "role";

    private final SetupQueryConsumer setupQueryConsumer;
    private final QueryExampleConsumer queryExampleConsumer;
    private final QueryExampleNoResultConsumer queryExampleNoResultConsumer;
    private final Runnable cleanup;

    private List<String> graphCreateQueries;

    public QueryConsumingTreeProcessor(
        SetupQueryConsumer setupQueryConsumer,
        QueryExampleConsumer queryExampleConsumer,
        QueryExampleNoResultConsumer queryExampleNoResultConsumer,
        Runnable cleanup
    ) {
        this.setupQueryConsumer = setupQueryConsumer;
        this.queryExampleConsumer = queryExampleConsumer;
        this.queryExampleNoResultConsumer = queryExampleNoResultConsumer;
        this.cleanup = cleanup;

        graphCreateQueries = new ArrayList<>();
    }

    @Override
    public Document process(Document document) {
        consumeSetupQueries(document);
        graphCreateQueries = collectSetupQueries(document, GRAPH_CREATE_QUERY_ROLE);
        consumeQueryExamples(document);
        return document;
    }

    private void consumeSetupQueries(Document document) {
        List<String> setupQueries = collectSetupQueries(document, SETUP_QUERY_ROLE);
        setupQueryConsumer.consume(setupQueries);
    }

    private List<String> collectSetupQueries(StructuralNode node, String setupQueryType) {
        List<StructuralNode> nodes = node.findBy(Map.of(ROLE_SELECTOR, setupQueryType));
        return nodes
            .stream()
            .map(StructuralNode::getContent)
            .map(Object::toString)
            .map(QueryConsumingTreeProcessor::undoReplacements)
            .collect(Collectors.toList());

    }

    private void consumeQueryExamples(StructuralNode node) {
        Collection<StructuralNode> queryExamples = new ArrayList<>();
        Collection<StructuralNode> queryNoResultExamples = new ArrayList<>();
        Map<String, List<StructuralNode>> groupedQueryExamples = new HashMap<>();

        collectQueryExamples(node, queryExamples, queryNoResultExamples, groupedQueryExamples);

        queryExamples.forEach(q -> processExample(() -> processCypherExample(q)));
        queryNoResultExamples.forEach(q -> processExample(() -> processCypherNoResultExample(q)));
        processGroupedQueryExamples(groupedQueryExamples);
    }

    private void collectQueryExamples(
        StructuralNode node,
        Collection<StructuralNode> queryExamples,
        Collection<StructuralNode> queryNoResultExamples,
        Map<String, List<StructuralNode>> groupedQueryExamples
    ) {
        List<StructuralNode> allQueryExamples = node.findBy(Map.of(ROLE_SELECTOR, QUERY_EXAMPLE_ROLE));
        allQueryExamples.forEach(queryExample -> {
            Object testGroupAttribute = queryExample.getAttribute(TEST_GROUP_ATTRIBUTE);
            if (testGroupAttribute != null) {
                String testGroup = testGroupAttribute.toString();
                groupedQueryExamples.putIfAbsent(testGroup, new ArrayList<>());
                groupedQueryExamples.get(testGroup).add(queryExample);
            } else {
                if (isNoResultExample(queryExample)) {
                    queryNoResultExamples.add(queryExample);
                } else {
                    queryExamples.add(queryExample);
                }
            }
        });
    }

    private void processGroupedQueryExamples(Map<String, List<StructuralNode>> groupedQueryExamples) {
        groupedQueryExamples.forEach((group, examples) -> {
            Collection<Runnable> groupQueries = new ArrayList<>();
            examples.forEach(example -> {
                if (isNoResultExample(example)) {
                    groupQueries.add(() -> processCypherNoResultExample(example));
                } else {
                    groupQueries.add(() -> processCypherExample(example));
                }
            });

            processExamples(groupQueries);
        });
    }

    private boolean isNoResultExample(ContentNode example) {
        return example.hasAttribute(TEST_TYPE_NO_RESULT) &&
               Boolean.parseBoolean(example.getAttribute(TEST_TYPE_NO_RESULT).toString());
    }

    private void processExample(Runnable example) {
        setupQueryConsumer.consume(graphCreateQueries);
        example.run();
        cleanup.run();
    }

    private void processExamples(Iterable<Runnable> examples) {
        setupQueryConsumer.consume(graphCreateQueries);
        examples.forEach(Runnable::run);
        cleanup.run();
    }

    private void processCypherExample(StructuralNode cypherExample) {
        Table resultTable = (Table) findByContext(cypherExample, TABLE_CONTEXT);
        List<String> headers = resultTable != null ? headers(resultTable) : Collections.emptyList();
        List<Row> rows = resultTable != null ? resultTable.getBody() : Collections.emptyList();
        queryExampleConsumer.consume(getCypherQuery(cypherExample), headers, rows);
    }

    private void processCypherNoResultExample(StructuralNode cypherExample) {
        queryExampleNoResultConsumer.consume(getCypherQuery(cypherExample));
    }

    private String getCypherQuery(StructuralNode cypherExample) {
        return undoReplacements(findByContext(cypherExample, CODE_BLOCK_CONTEXT).getContent().toString());
    }

    private StructuralNode findByContext(StructuralNode node, String context) {
        return node
            .findBy(Map.of("context", context))
            .stream()
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException(formatWithLocale("No nodes found for context '%s'", context)));
    }

    private List<String> headers(Table table) {
        return table.getHeader().isEmpty()
            ? Collections.emptyList()
            : table.getHeader().get(0).getCells().stream().map(Cell::getText).collect(Collectors.toList());
    }

    private static String undoReplacements(String content) {
        return content
            .replace("&gt;", ">")
            .replace("&lt;", "<");
    }

    @FunctionalInterface
    interface QueryExampleConsumer {
        void consume(String query, List<String> columns, List<Row> rows);
    }

    @FunctionalInterface
    interface QueryExampleNoResultConsumer {
        void consume(String query);
    }

    @FunctionalInterface
    interface SetupQueryConsumer {
        void consume(List<String> setupQueries);
    }
}
