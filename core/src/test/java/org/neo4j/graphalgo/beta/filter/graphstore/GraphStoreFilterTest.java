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
package org.neo4j.graphalgo.beta.filter.graphstore;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.graphalgo.beta.filter.expr.Expression.Literal.TrueLiteral;
import org.opencypher.v9_0.parser.javacc.ParseException;
import org.neo4j.graphalgo.api.GraphStore;
import org.neo4j.graphalgo.beta.filter.expr.Expression;
import org.neo4j.graphalgo.beta.filter.expr.ExpressionParser;
import org.neo4j.graphalgo.extension.GdlExtension;
import org.neo4j.graphalgo.extension.GdlGraph;
import org.neo4j.graphalgo.extension.IdFunction;
import org.neo4j.graphalgo.extension.Inject;

import static org.neo4j.graphalgo.TestSupport.assertGraphEquals;
import static org.neo4j.graphalgo.TestSupport.fromGdl;
import static org.neo4j.graphalgo.beta.filter.graphstore.GraphStoreFilter.subGraph;

@GdlExtension
class GraphStoreFilterTest {

    @GdlGraph
    public static final String GDL = "(a:A {foo: 42})-[:REL]->(b:B { foo: 84 })";
    @GdlGraph(graphNamePrefix = "relProps")
    public static final String GDL2 = "(a:A {foo: 42})-[:REL { w: 42.0 }]->(b:B { foo: 84 })";

    @Inject
    private GraphStore graphStore;

    @Inject
    private GraphStore relPropsGraphStore;

    @Inject
    private IdFunction idFunction;

    @Test
    void propertyFilterAnd() throws ParseException {
        var nodeExpr = ExpressionParser.parse("n.foo > 42 AND n.foo <= 84");
        var filteredGraphStore = subGraph(graphStore, nodeExpr, TrueLiteral.INSTANCE);
        assertGraphEquals(fromGdl("(:B {foo: 84})"), filteredGraphStore.getUnion());
    }

    @ParameterizedTest
    @ValueSource(strings = {"true OR false AND true", "true AND false OR true"})
    void propertyFilterOrAnd(String expression) throws ParseException {
        var nodeExpr = ExpressionParser.parse(expression);
        var filteredGraphStore = subGraph(graphStore, nodeExpr, TrueLiteral.INSTANCE);
        assertGraphEquals(fromGdl("(a:A {foo: 42})-[:REL]->(b:B { foo: 84 })"), filteredGraphStore.getUnion());
    }

    @Test
    void singleLabelFilter() throws ParseException {
        var nodeExpr = ExpressionParser.parse("n:A");
        var filteredGraphStore = subGraph(graphStore, nodeExpr, TrueLiteral.INSTANCE);
        assertGraphEquals(fromGdl("(:A {foo: 42})"), filteredGraphStore.getUnion());
    }

    @Test
    void singleRelationshipFilter() throws ParseException {
        var filteredGraphStore = subGraph(
            graphStore,
            TrueLiteral.INSTANCE,
            ExpressionParser.parse("r:REL")
        );
        assertGraphEquals(fromGdl("(a:A {foo: 42})-[:REL]->(b:B { foo: 84 })"), filteredGraphStore.getUnion());
    }

    @Test
    void singleRelationshipFilterWithProperty() throws ParseException {
        var filteredGraphStore = subGraph(
            relPropsGraphStore,
            TrueLiteral.INSTANCE,
            ExpressionParser.parse("r:REL AND r.w >= 42.0")
        );
        assertGraphEquals(
            fromGdl("(a:A {foo: 42})-[:REL { w: 42.0 }]->(b:B { foo: 84 })"),
            filteredGraphStore.getUnion()
        );
    }
}
