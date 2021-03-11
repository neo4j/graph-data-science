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
package org.neo4j.graphalgo.beta.filter.expr;

import org.neo4j.cypher.internal.ast.factory.ASTExceptionFactory;
import org.neo4j.cypher.internal.ast.factory.ASTFactory;
import org.neo4j.cypher.internal.ast.factory.ASTFactory.NULL;
import org.neo4j.cypher.internal.parser.javacc.Cypher;
import org.neo4j.cypher.internal.parser.javacc.CypherCharStream;
import org.neo4j.cypher.internal.parser.javacc.ParseException;

import java.util.List;

public class ExpressionParser {

    public Expression parse(String cypher) throws ParseException {
        var astFactory = new GDSASTFactory();
        var exceptionFactory = new DummyExceptionFactory();
        var charstream = new CypherCharStream(cypher);

        return new Cypher<>(astFactory, exceptionFactory, charstream).Expression();
    }

    static class DummyExceptionFactory implements ASTExceptionFactory {

        @Override
        public Exception syntaxException(
            String got,
            List<String> expected,
            Exception source,
            int offset,
            int line,
            int column
        ) {
            return new IllegalArgumentException("wrong syntax brah");
        }

        @Override
        public Exception syntaxException(Exception source, int offset, int line, int column) {
            return new IllegalArgumentException("wrong syntax brah");
        }
    }


     static class GDSASTFactory extends GDSExpressionFactory implements ASTFactory<NULL, NULL, NULL, NULL, NULL, NULL, Object, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, Expression, Expression.Variable, Expression.Property, Object, InputPosition> {

         @Override
         public NULL newSingleQuery(List<NULL> nulls) {
             return null;
         }

         @Override
         public NULL newUnion(InputPosition p, NULL lhs, NULL rhs, boolean all) {
             return null;
         }

         @Override
         public NULL periodicCommitQuery(
             InputPosition p, String batchSize, NULL loadCsv, List<NULL> queryBody
         ) {
             return null;
         }

         @Override
         public NULL fromClause(InputPosition p, Expression e) {
             return null;
         }

         @Override
         public NULL useClause(InputPosition p, Expression e) {
             return null;
         }

         @Override
         public NULL newReturnClause(
             InputPosition p,
             boolean distinct,
             boolean returnAll,
             List<NULL> nulls,
             List<NULL> order,
             Expression skip,
             Expression limit
         ) {
             return null;
         }

         @Override
         public NULL newReturnGraphClause(InputPosition p) {
             return null;
         }

         @Override
         public NULL newReturnItem(InputPosition p, Expression e, Expression.Variable v) {
             return null;
         }

         @Override
         public NULL newReturnItem(InputPosition p, Expression e, int eStartOffset, int eEndOffset) {
             return null;
         }

         @Override
         public NULL orderDesc(Expression e) {
             return null;
         }

         @Override
         public NULL orderAsc(Expression e) {
             return null;
         }

         @Override
         public NULL withClause(InputPosition p, NULL aNull, Expression where) {
             return null;
         }

         @Override
         public NULL matchClause(
             InputPosition p, boolean optional, List<Object> objects, List<NULL> nulls, Expression where
         ) {
             return null;
         }

         @Override
         public NULL usingIndexHint(
             InputPosition p, Expression.Variable v, String label, List<String> properties, boolean seekOnly
         ) {
             return null;
         }

         @Override
         public NULL usingJoin(
             InputPosition p, List<Expression.Variable> joinVariables
         ) {
             return null;
         }

         @Override
         public NULL usingScan(InputPosition p, Expression.Variable v, String label) {
             return null;
         }

         @Override
         public NULL createClause(InputPosition p, List<Object> objects) {
             return null;
         }

         @Override
         public NULL setClause(InputPosition p, List<NULL> nulls) {
             return null;
         }

         @Override
         public NULL setProperty(Expression.Property property, Expression value) {
             return null;
         }

         @Override
         public NULL setVariable(Expression.Variable variable, Expression value) {
             return null;
         }

         @Override
         public NULL addAndSetVariable(Expression.Variable variable, Expression value) {
             return null;
         }

         @Override
         public NULL setLabels(
             Expression.Variable variable, List<StringPos<InputPosition>> value
         ) {
             return null;
         }

         @Override
         public NULL removeClause(InputPosition p, List<NULL> nulls) {
             return null;
         }

         @Override
         public NULL removeProperty(Expression.Property property) {
             return null;
         }

         @Override
         public NULL removeLabels(
             Expression.Variable variable, List<StringPos<InputPosition>> labels
         ) {
             return null;
         }

         @Override
         public NULL deleteClause(
             InputPosition p, boolean detach, List<Expression> expressions
         ) {
             return null;
         }

         @Override
         public NULL unwindClause(InputPosition p, Expression e, Expression.Variable v) {
             return null;
         }

         @Override
         public NULL mergeClause(
             InputPosition p, Object o, List<NULL> nulls, List<MergeActionType> actionTypes
         ) {
             return null;
         }

         @Override
         public NULL callClause(
             InputPosition p,
             List<String> namespace,
             String name,
             List<Expression> arguments,
             List<NULL> nulls,
             Expression where
         ) {
             return null;
         }

         @Override
         public NULL callResultItem(InputPosition p, String name, Expression.Variable v) {
             return null;
         }

         @Override
         public Object namedPattern(Expression.Variable v, Object o) {
             return null;
         }

         @Override
         public Object shortestPathPattern(InputPosition p, Object o) {
             return null;
         }

         @Override
         public Object allShortestPathsPattern(InputPosition p, Object o) {
             return null;
         }

         @Override
         public Object everyPathPattern(
             List<NULL> nodes, List<NULL> relationships
         ) {
             return null;
         }

         @Override
         public NULL nodePattern(
             InputPosition p, Expression.Variable v, List<StringPos<InputPosition>> labels, Expression properties
         ) {
             return null;
         }

         @Override
         public NULL relationshipPattern(
             InputPosition p,
             boolean left,
             boolean right,
             Expression.Variable v,
             List<StringPos<InputPosition>> relTypes,
             NULL aNull,
             Expression properties,
             boolean legacyTypeSeparator
         ) {
             return null;
         }

         @Override
         public NULL pathLength(InputPosition p, String minLength, String maxLength) {
             return null;
         }

         @Override
         public NULL loadCsvClause(
             InputPosition p, boolean headers, Expression source, Expression.Variable v, String fieldTerminator
         ) {
             return null;
         }

         @Override
         public NULL foreachClause(
             InputPosition p, Expression.Variable v, Expression list, List<NULL> nulls
         ) {
             return null;
         }

         @Override
         public NULL subqueryClause(InputPosition p, NULL subquery) {
             return null;
         }
     }
}
