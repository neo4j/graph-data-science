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
package org.neo4j.gds;

import org.assertj.core.api.HamcrestCondition;
import org.hamcrest.Matcher;
import org.intellij.lang.annotations.Language;
import org.intellij.lang.annotations.RegExp;
import org.junit.jupiter.api.AfterEach;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.compat.GraphDatabaseApiProxy;
import org.neo4j.gds.core.ExceptionMessageMatcher;
import org.neo4j.gds.core.Username;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.kernel.api.procedure.CallableUserAggregationFunction;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.CoreMatchers.allOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class BaseProcTest extends BaseTest {

    @AfterEach
    void cleanupGraphStoreCatalog() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    protected void registerFunctions(Class<?>... functionClasses) throws Exception {
        GraphDatabaseApiProxy.registerFunctions(db, functionClasses);
    }

    protected void registerAggregationFunctions(Class<?>... functionClasses) throws Exception {
        GraphDatabaseApiProxy.registerAggregationFunctions(db, functionClasses);
    }

    protected void registerAggregationFunction(CallableUserAggregationFunction function) throws Exception {
        GraphDatabaseApiProxy.register(db, function);
    }

    protected void registerFunctions(GraphDatabaseService db, Class<?>... functionClasses) throws Exception {
        GraphDatabaseApiProxy.registerFunctions(db, functionClasses);
    }

    protected void registerProcedures(Class<?>... procedureClasses) throws Exception {
        registerProcedures(db, procedureClasses);
    }

    protected void registerProcedures(GraphDatabaseService db, Class<?>... procedureClasses) throws Exception {
        GraphDatabaseApiProxy.registerProcedures(db, procedureClasses);
    }

    <T> T resolveDependency(Class<T> dependency) {
        return GraphDatabaseApiProxy.resolveDependency(db, dependency);
    }

    protected String getUsername() {
        return Username.EMPTY_USERNAME.username();
    }

    protected void assertError(
        @Language("Cypher") String query,
        String messageSubstring
    ) {
        assertError(query, emptyMap(), messageSubstring);
    }

    protected void assertError(
        @Language("Cypher") String query,
        Map<String, Object> queryParameters,
        String messageSubstring
    ) {
        assertError(query, queryParameters, ExceptionMessageMatcher.containsMessage(messageSubstring));
    }

    protected void assertError(
        @Language("Cypher") String query,
        Map<String, Object> queryParameters,
        List<String> messageSubstrings
    ) {
        assertError(
            query,
            queryParameters,
            allOf(messageSubstrings.stream()
                .map(ExceptionMessageMatcher::containsMessage)
                .collect(Collectors.toList()))
        );
    }

    protected void assertErrorRegex(
        @Language("Cypher") String query,
        @RegExp String regex
    ) {
        assertErrorRegex(query, emptyMap(), regex);
    }

    private void assertErrorRegex(
        @Language("Cypher") String query,
        Map<String, Object> queryParameters,
        @RegExp String regex
    ) {
        assertError(query, queryParameters, ExceptionMessageMatcher.containsMessageRegex(regex));
    }

    private void assertError(
        @Language("Cypher") String query,
        Map<String, Object> queryParameters,
        Matcher<Throwable> matcher
    ) {
        try {
            runQueryWithResultConsumer(query, queryParameters, BaseProcTest::consume);
            fail(formatWithLocale("Expected an exception to be thrown by query:\n%s", query));
        } catch (Throwable e) {
            assertThat(e).has(new HamcrestCondition<>(matcher));
        }
    }

    protected void assertUserInput(Result.ResultRow row, String key, Object expected) {
        Map<String, Object> configMap = extractUserInput(row);
        assertTrue(configMap.containsKey(key), formatWithLocale("Key %s is not present in config", key));
        assertEquals(expected, configMap.get(key));
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> extractUserInput(Result.ResultRow row) {
        return ((Map<String, Object>) row.get("configuration"));
    }

    protected void loadCompleteGraph(String graphName) {
        loadCompleteGraph(graphName, Orientation.NATURAL);
    }

    protected void loadCompleteGraph(String graphName, Orientation orientation) {
        var createQuery = GdsCypher.call(graphName)
            .graphProject()
            .loadEverything(orientation)
            .yields();
        runQuery(createQuery);
    }

    protected void assertGraphExists(String graphName) {
        Set<Graph> graphs = getLoadedGraphs(graphName);
        assertEquals(1, graphs.size());
    }

    protected void assertGraphDoesNotExist(String graphName) {
        Set<Graph> graphs = getLoadedGraphs(graphName);
        assertTrue(graphs.isEmpty());
    }

    protected Graph findLoadedGraph(String graphName) {
        return GraphStoreCatalog
            .getGraphStores("", DatabaseId.of(db))
            .entrySet()
            .stream()
            .filter(e -> e.getKey().graphName().equals(graphName))
            .map(e -> e.getValue().getUnion())
            .findFirst()
            .orElseThrow(() -> new RuntimeException(formatWithLocale("Graph %s not found.", graphName)));
    }

    private Set<Graph> getLoadedGraphs(String graphName) {
        return GraphStoreCatalog
            .getGraphStores("", DatabaseId.of(db))
            .entrySet()
            .stream()
            .filter(e -> e.getKey().graphName().equals(graphName))
            .map(e -> e.getValue().getUnion())
            .collect(Collectors.toSet());
    }

    private static void consume(ResourceIterator<Map<String, Object>> result) {
        while (result.hasNext()) {
            result.next();
        }
        result.close();
    }
}
