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
import org.asciidoctor.ast.Document;
import org.asciidoctor.ast.Row;
import org.asciidoctor.ast.StructuralNode;
import org.asciidoctor.ast.Table;
import org.asciidoctor.extension.Treeprocessor;
import org.jetbrains.annotations.NotNull;
import org.neo4j.gds.doc.syntax.DocQuery;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.neo4j.gds.doc.DocumentationTestToolsConstants.CODE_BLOCK_CONTEXT;
import static org.neo4j.gds.doc.DocumentationTestToolsConstants.GRAPH_PROJECT_QUERY_ROLE;
import static org.neo4j.gds.doc.DocumentationTestToolsConstants.QUERY_EXAMPLE_ROLE;
import static org.neo4j.gds.doc.DocumentationTestToolsConstants.ROLE_SELECTOR;
import static org.neo4j.gds.doc.DocumentationTestToolsConstants.SETUP_QUERY_ROLE;
import static org.neo4j.gds.doc.DocumentationTestToolsConstants.TABLE_CONTEXT;
import static org.neo4j.gds.doc.DocumentationTestToolsConstants.TEST_GROUP_ATTRIBUTE;
import static org.neo4j.gds.doc.DocumentationTestToolsConstants.TEST_OPERATOR_ATTRIBUTE;
import static org.neo4j.gds.doc.DocumentationTestToolsConstants.TEST_TYPE_NO_RESULT;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

/**
 * Extension for Asciidoctor. Think of it as a listener: it is installed in the AsciiDoctor instance and gets pinged
 * once for each file that is processed.
 *
 * It looks for two kinds of blocks in the AsciiDoc source:
 * 1) Blocks with the role `setup-query` or 'graph-project-query'
 * 2) Blocks with the role `query-example`. The content is expected to contain exactly two blocks:
 * 2.1) First, a block containing a Cypher query example.
 * 2.2) Second, a table containing a Cypher query result.
 * Each block may have an attribute 'operator' if we want a certain user to execute the query
 *
 * Once you have called {@link org.neo4j.gds.doc.QueryCollectingTreeProcessor#process(org.asciidoctor.ast.Document)}
 * you are then able to query this class for the before-all, before-each and actual example queries.
 *
 * Remember though, you should only harvest queries after the last document has been processed, or you will not get the
 * right grouping.
 */
public class QueryCollectingTreeProcessor extends Treeprocessor {
    private final List<DocQuery> beforeAllQueries = new ArrayList<>();

    private final List<DocQuery> beforeEachQueries = new ArrayList<>();

    /**
     * "Group encounter order" is preserved if we use a LinkedHashMap, and while that is not strictly necessary, I think
     * it is a nice touch. Not sure if JUnit UI reflects it, TBD.
     */
    private final Map<String, List<QueryExample>> queryExamples = new LinkedHashMap<>();

    /**
     * This method is called once for each file loaded. This affects grouping mainly.
     *
     * We collect the three types of queries from each file, and someone can harvest them all later.
     */
    @Override
    public Document process(Document document) {
        collectBeforeAllQueries(document);
        collectBeforeEachQueries(document);
        collectQueryExamples(document);

        return document;
    }

    public List<DocQuery> getBeforeAllQueries() {
        return beforeAllQueries;
    }

    public List<DocQuery> getBeforeEachQueries() {
        return beforeEachQueries;
    }

    /**
     * Do grouping into QueryExampleGroups
     */
    public List<QueryExampleGroup> getQueryExampleGroups() {
        return queryExamples.entrySet().stream()
            .map(e -> QueryExampleGroup.builder()
                .displayName(e.getKey())
                .queryExamples(e.getValue())
                .build())
            .collect(Collectors.toList());
    }

    private void collectBeforeAllQueries(StructuralNode document) {
        var queries = CollectSetupQueries(document, SETUP_QUERY_ROLE);
        beforeAllQueries.addAll(queries);
    }

    private void collectBeforeEachQueries(StructuralNode document) {
        var queries = CollectSetupQueries(document, GRAPH_PROJECT_QUERY_ROLE);
        beforeEachQueries.addAll(queries);
    }

    private static List<DocQuery> CollectSetupQueries(StructuralNode node, String setupQueryType) {
        var nodes = node.findBy(Map.of(ROLE_SELECTOR, setupQueryType));

        return nodes.stream()
            .map(QueryCollectingTreeProcessor::ParseDocQuery)
            .collect(Collectors.toList());
    }

    private static DocQuery ParseDocQuery(StructuralNode structuralNode) {
        return DocQuery.builder()
            .query(parseQuery(structuralNode))
            .operator(parseOperator(structuralNode))
            .build();
    }

    private static String parseOperator(StructuralNode structuralNode) {
        return structuralNode.getAttribute(TEST_OPERATOR_ATTRIBUTE, DocQuery.DEFAULT_OPERATOR).toString();
    }

    private void collectQueryExamples(StructuralNode node) {
        var examples = node.findBy(Map.of(ROLE_SELECTOR, QUERY_EXAMPLE_ROLE));

        for (StructuralNode example : examples) {
            var displayName = extractDisplayName(example);
            var queryExample = convertToQueryExample(example);
            addQueryExample(displayName, queryExample);
        }
    }

    private void addQueryExample(String displayName, QueryExample queryExample) {
        queryExamples.computeIfAbsent(displayName, key -> new ArrayList<>());
        queryExamples.get(displayName).add(queryExample);
    }

    private QueryExample convertToQueryExample(StructuralNode queryExampleNode) {
        var codeBlock = findByContext(queryExampleNode, CODE_BLOCK_CONTEXT);
        var query = parseQuery(codeBlock);

        var queryExampleBuilder = QueryExample.builder().query(query);

        if (queryExampleNode.hasAttribute(TEST_OPERATOR_ATTRIBUTE)) {
            queryExampleBuilder.operator(queryExampleNode.getAttribute(TEST_OPERATOR_ATTRIBUTE).toString());
        }

        if (Boolean.parseBoolean(queryExampleNode.getAttribute(TEST_TYPE_NO_RESULT, false).toString())) {
            queryExampleBuilder.assertResults(false);
        } else {
            parseResultTable(queryExampleNode, queryExampleBuilder);
        }

        return queryExampleBuilder.build();
    }

    private void parseResultTable(StructuralNode queryExampleNode, ImmutableQueryExample.Builder queryExampleBuilder) {
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
                    .map(QueryCollectingTreeProcessor::undoReplacements)
                    .collect(Collectors.toList())
            );
        }
    }

    /**
     * We need these for both grouping and for JUnit UI
     *
     * Grouping is captured directly - a group _is_ a name.
     *
     * Everything else is JUnit UI where it'd be nice to have something human readable.
     *
     * But! We also need these to _act_ as group names - or not.
     *
     * We count on being able to code group names. Code block titles also as a fallback.
     *
     * But for the queries, imagine a case where we want to run two identical queries... Far fetched, but we
     * disambiguate just because that is easy.
     */
    private String extractDisplayName(StructuralNode queryExample) {
        var groupAttribute = queryExample.getAttribute(TEST_GROUP_ATTRIBUTE);
        if (groupAttribute != null) return groupAttribute.toString();

        var codeBlock = findByContext(queryExample, CODE_BLOCK_CONTEXT);
        if (codeBlock.getTitle() != null) return codeBlock.getTitle();

        return parseQuery(codeBlock) + " - " + UUID.randomUUID().toString().substring(0, 5);
    }

    @NotNull
    private static String parseQuery(StructuralNode structuralNode) {
        return undoReplacements(structuralNode.getContent().toString());
    }

    /**
     * Nested search inside a structural node for the given context.
     *
     * Example would be the code block inside a query example.
     */
    private StructuralNode findByContext(StructuralNode node, String context) {
        return node.findBy(Map.of("context", context)).stream()
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException(formatWithLocale(
                "No nodes found for context '%s'",
                context
            )));
    }

    private static String undoReplacements(String content) {
        return content
            .replace("&gt;", ">")
            .replace("&lt;", "<");
    }
}
