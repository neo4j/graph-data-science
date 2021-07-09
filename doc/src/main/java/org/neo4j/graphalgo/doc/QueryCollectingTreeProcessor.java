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

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

/**
 * Extension for Asciidoctor.
 * It looks for two kinds of blocks in the AsciiDoc source:
 * 1) Blocks with the role `setup-query`.
 * 2) Blocks with the role `query-example`. The content is expected to contain exactly two blocks:
 * 2.1) First, a block containing a Cypher query example.
 * 2.2) Second, a table containing a Cypher query result.
 */
public class QueryCollectingTreeProcessor extends Treeprocessor {

    private static final String CODE_BLOCK_CONTEXT = ":listing";
    private static final String TABLE_CONTEXT = ":table";

    private static final String SETUP_QUERY_ROLE = "setup-query";
    private static final String GRAPH_CREATE_QUERY_ROLE = "graph-create-query";
    private static final String QUERY_EXAMPLE_ROLE = "query-example";
    private static final String TEST_TYPE_NO_RESULT = "no-result";
    private static final String TEST_GROUP_ATTRIBUTE = "group";
    private static final String ROLE_SELECTOR = "role";

    private List<String> beforeAllQueries;
    private List<String> beforeEachQueries;
    private List<QueryExampleGroup> queryExamples;

    public List<String> beforeAllQueries() {
        return beforeAllQueries;
    }

    public List<String> beforeEachQueries() {
        return beforeEachQueries;
    }

    public List<QueryExampleGroup> queryExamples() {
        return queryExamples;
    }

    @Override
    public Document process(Document document) {
        beforeAllQueries = collectBeforeAllQueries(document);
        beforeEachQueries = collectBeforeEachQueries(document);
        queryExamples = collectQueryExamples(document);
        return document;
    }

    private List<String> collectBeforeAllQueries(StructuralNode document) {
        return collectSetupQueries(document, SETUP_QUERY_ROLE);
    }

    private List<String> collectBeforeEachQueries(StructuralNode document) {
        return collectSetupQueries(document, GRAPH_CREATE_QUERY_ROLE);
    }

    private List<String> collectSetupQueries(StructuralNode node, String setupQueryType) {
        List<StructuralNode> nodes = node.findBy(Map.of(ROLE_SELECTOR, setupQueryType));
        return nodes
            .stream()
            .map(StructuralNode::getContent)
            .map(Object::toString)
            .map(this::undoReplacements)
            .collect(Collectors.toList());

    }

    private List<QueryExampleGroup> collectQueryExamples(StructuralNode node) {
        var queryExampleNodes = node.findBy(Map.of(ROLE_SELECTOR, QUERY_EXAMPLE_ROLE));
        var groupedQueryExampleNodes = collectQueryExampleNodes(queryExampleNodes);

        List<QueryExampleGroup> queryExampleGroups = new ArrayList<>();

        groupedQueryExampleNodes.forEach((displayName, groupedQueryExamples) -> {
            var groupBuilder = QueryExampleGroup.builder().displayName(displayName);

            groupedQueryExamples.forEach(queryExampleNode -> {
                var codeBlock = findByContext(queryExampleNode, CODE_BLOCK_CONTEXT);
                var query = undoReplacements(codeBlock.getContent().toString());

                var queryExampleBuilder = QueryExample.builder().query(query);

                if (Boolean.parseBoolean(queryExampleNode.getAttribute(TEST_TYPE_NO_RESULT, false).toString())) {
                    queryExampleBuilder.assertResults(false);
                } else {
                    var resultsTable = (Table) findByContext(queryExampleNode, TABLE_CONTEXT);

                    var resultColumns = resultsTable.getHeader().get(0).getCells()
                        .stream()
                        .map(Cell::getText)
                        .collect(Collectors.toList());

                    queryExampleBuilder.resultColumns(resultColumns);

                    var body = resultsTable.getBody();
                    for (Row resultRow : body) {
                        queryExampleBuilder.addResult(
                            resultRow.getCells()
                                .stream()
                                .map(Cell::getText)
                                .map(this::undoReplacements)
                                .collect(Collectors.toList())
                        );
                    }
                }

                groupBuilder.addQueryExample(queryExampleBuilder.build());

            });

            queryExampleGroups.add(groupBuilder.build());
        });

        return queryExampleGroups;
    }

    private HashMap<String, List<StructuralNode>> collectQueryExampleNodes(Iterable<StructuralNode> queryExampleNodes) {
        var groupedQueryExampleNodes = new HashMap<String, List<StructuralNode>>();
        queryExampleNodes.forEach(queryExampleNode -> {
            var testDisplayName = extractDisplayName(queryExampleNode);
            groupedQueryExampleNodes.putIfAbsent(testDisplayName, new ArrayList<>());
            groupedQueryExampleNodes.get(testDisplayName).add(queryExampleNode);
        });

        return groupedQueryExampleNodes;
    }

    private String extractDisplayName(StructuralNode queryExampleNode) {
        var testGroupAttribute = queryExampleNode.getAttribute(TEST_GROUP_ATTRIBUTE);
        String testDisplayName;
        if (testGroupAttribute != null) {
            testDisplayName = testGroupAttribute.toString();
        } else {
            var codeBlock = findByContext(queryExampleNode, CODE_BLOCK_CONTEXT);
            var query = undoReplacements(codeBlock.getContent().toString());

            testDisplayName = codeBlock.getTitle() == null
                ? query
                : codeBlock.getTitle();
        }
        return testDisplayName;
    }

    private StructuralNode findByContext(StructuralNode node, String context) {
        return node
            .findBy(Map.of("context", context))
            .stream()
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException(formatWithLocale(
                "No nodes found for context '%s'",
                context
            )));
    }

    private String undoReplacements(String content) {
        return content
            .replaceAll("&gt;", ">")
            .replaceAll("&lt;", "<");
    }
}
