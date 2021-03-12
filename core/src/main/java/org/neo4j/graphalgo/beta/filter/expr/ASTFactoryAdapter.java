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

import org.opencypher.v9_0.ast.factory.ASTFactory;
import org.opencypher.v9_0.ast.factory.ASTFactory.NULL;
import scala.util.Either;

import java.util.List;

abstract class ASTFactoryAdapter implements ASTFactory<NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    Expression,
    Expression,
    Expression.LeafExpression.Variable,
    Expression.UnaryExpression.Property,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    InputPosition> {

    @Override
    public Expression regeq(InputPosition p, Expression lhs, Expression rhs) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression startsWith(InputPosition p, Expression lhs, Expression rhs) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression endsWith(InputPosition p, Expression lhs, Expression rhs) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression contains(InputPosition p, Expression lhs, Expression rhs) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression in(InputPosition p, Expression lhs, Expression rhs) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression isNull(Expression e) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression listLookup(Expression list, Expression index) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression listSlice(InputPosition p, Expression list, Expression start, Expression end) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression newCountStar(InputPosition p) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression functionInvocation(
        InputPosition p,
        List<String> namespace,
        String name,
        boolean distinct,
        List<Expression> arguments
    ) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression listComprehension(
        InputPosition p,
        Expression.LeafExpression.Variable v,
        Expression list,
        Expression where,
        Expression projection
    ) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression patternComprehension(
        InputPosition p,
        Expression.LeafExpression.Variable v,
        NULL aNull,
        Expression where,
        Expression projection
    ) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression filterExpression(InputPosition p, Expression.LeafExpression.Variable v, Expression list, Expression where) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression extractExpression(
        InputPosition p,
        Expression.LeafExpression.Variable v,
        Expression list,
        Expression where,
        Expression projection
    ) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression reduceExpression(
        InputPosition p,
        Expression.LeafExpression.Variable acc,
        Expression accExpr,
        Expression.LeafExpression.Variable v,
        Expression list,
        Expression innerExpr
    ) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression allExpression(InputPosition p, Expression.LeafExpression.Variable v, Expression list, Expression where) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression anyExpression(InputPosition p, Expression.LeafExpression.Variable v, Expression list, Expression where) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression noneExpression(InputPosition p, Expression.LeafExpression.Variable v, Expression list, Expression where) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression singleExpression(InputPosition p, Expression.LeafExpression.Variable v, Expression list, Expression where) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression patternExpression(InputPosition p, NULL aNull) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression existsSubQuery(InputPosition p, List<NULL> nulls, Expression where) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression mapProjection(InputPosition p, Expression.LeafExpression.Variable v, List<NULL> nulls) {
        throw new UnsupportedOperationException();
    }

    @Override
    public NULL mapProjectionLiteralEntry(ASTFactory.StringPos<InputPosition> property, Expression value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public NULL mapProjectionProperty(ASTFactory.StringPos<InputPosition> property) {
        throw new UnsupportedOperationException();
    }

    @Override
    public NULL mapProjectionVariable(Expression.LeafExpression.Variable v) {
        throw new UnsupportedOperationException();
    }

    @Override
    public NULL mapProjectionAll(InputPosition p) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression caseExpression(
        InputPosition p,
        Expression e,
        List<Expression> whens,
        List<Expression> thens,
        Expression elze
    ) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression plus(InputPosition p, Expression lhs, Expression rhs) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression minus(InputPosition p, Expression lhs, Expression rhs) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression multiply(InputPosition p, Expression lhs, Expression rhs) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression divide(InputPosition p, Expression lhs, Expression rhs) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression modulo(InputPosition p, Expression lhs, Expression rhs) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression pow(InputPosition p, Expression lhs, Expression rhs) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression unaryPlus(Expression e) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression unaryMinus(Expression e) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression newNullLiteral(InputPosition p) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression listLiteral(InputPosition p, List<Expression> values) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression mapLiteral(
        InputPosition p,
        List<ASTFactory.StringPos<InputPosition>> keys,
        List<Expression> values
    ) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression ands(List<Expression> exprs) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression newHexInteger(InputPosition p, String image, boolean negated) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression newOctalInteger(InputPosition p, String image, boolean negated) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression newString(InputPosition p, String image) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression newParameter(InputPosition p, String offset) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression newStringParameter(InputPosition p, Expression.LeafExpression.Variable v) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression newStringParameter(InputPosition p, String offset) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression oldParameter(InputPosition p, Expression.LeafExpression.Variable v) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression newParameter(InputPosition p, Expression.LeafExpression.Variable v) {
        throw new UnsupportedOperationException();
    }

    @Override
    public NULL newSingleQuery(List<NULL> nulls) {
        throw new UnsupportedOperationException();
    }

    @Override
    public NULL newUnion(InputPosition p, NULL lhs, NULL rhs, boolean all) {
        throw new UnsupportedOperationException();
    }

    @Override
    public NULL periodicCommitQuery(InputPosition p, String batchSize, NULL loadCsv, List<NULL> queryBody) {
        throw new UnsupportedOperationException();
    }

    @Override
    public NULL fromClause(InputPosition p, Expression e) {
        throw new UnsupportedOperationException();
    }

    @Override
    public NULL useClause(InputPosition p, Expression e) {
        throw new UnsupportedOperationException();
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
        throw new UnsupportedOperationException();
    }

    @Override
    public NULL newReturnGraphClause(InputPosition p) {
        throw new UnsupportedOperationException();
    }

    @Override
    public NULL newReturnItem(InputPosition p, Expression e, Expression.LeafExpression.Variable v) {
        throw new UnsupportedOperationException();
    }

    @Override
    public NULL newReturnItem(InputPosition p, Expression e, int eStartOffset, int eEndOffset) {
        throw new UnsupportedOperationException();
    }

    @Override
    public NULL orderDesc(Expression e) {
        throw new UnsupportedOperationException();
    }

    @Override
    public NULL orderAsc(Expression e) {
        throw new UnsupportedOperationException();
    }

    @Override
    public NULL withClause(InputPosition p, NULL aNull, Expression where) {
        throw new UnsupportedOperationException();
    }

    @Override
    public NULL matchClause(InputPosition p, boolean optional, List<NULL> nulls, List<NULL> nulls2, Expression where) {
        throw new UnsupportedOperationException();
    }

    @Override
    public NULL usingIndexHint(
        InputPosition p,
        Expression.LeafExpression.Variable v,
        String label,
        List<String> properties,
        boolean seekOnly
    ) {
        throw new UnsupportedOperationException();
    }

    @Override
    public NULL usingJoin(InputPosition p, List<Expression.LeafExpression.Variable> joinVariables) {
        throw new UnsupportedOperationException();
    }

    @Override
    public NULL usingScan(InputPosition p, Expression.LeafExpression.Variable v, String label) {
        throw new UnsupportedOperationException();
    }

    @Override
    public NULL createClause(InputPosition p, List<NULL> nulls) {
        throw new UnsupportedOperationException();
    }

    @Override
    public NULL setClause(InputPosition p, List<NULL> nulls) {
        throw new UnsupportedOperationException();
    }

    @Override
    public NULL setProperty(Expression.UnaryExpression.Property property, Expression value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public NULL setVariable(Expression.LeafExpression.Variable variable, Expression value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public NULL addAndSetVariable(Expression.LeafExpression.Variable variable, Expression value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public NULL setLabels(Expression.LeafExpression.Variable variable, List<StringPos<InputPosition>> value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public NULL removeClause(InputPosition p, List<NULL> nulls) {
        throw new UnsupportedOperationException();
    }

    @Override
    public NULL removeProperty(Expression.UnaryExpression.Property property) {
        throw new UnsupportedOperationException();
    }

    @Override
    public NULL removeLabels(Expression.LeafExpression.Variable variable, List<StringPos<InputPosition>> labels) {
        throw new UnsupportedOperationException();
    }

    @Override
    public NULL deleteClause(InputPosition p, boolean detach, List<Expression> expressions) {
        throw new UnsupportedOperationException();
    }

    @Override
    public NULL unwindClause(InputPosition p, Expression e, Expression.LeafExpression.Variable v) {
        throw new UnsupportedOperationException();
    }

    @Override
    public NULL mergeClause(InputPosition p, NULL aNull, List<NULL> nulls, List<MergeActionType> actionTypes) {
        throw new UnsupportedOperationException();
    }

    @Override
    public NULL callClause(
        InputPosition p,
        List<String> namespace,
        String name,
        List<Expression> arguments,
        boolean yieldAll,
        List<NULL> nulls,
        Expression where
    ) {
        throw new UnsupportedOperationException();
    }

    @Override
    public NULL callResultItem(InputPosition p, String name, Expression.LeafExpression.Variable v) {
        throw new UnsupportedOperationException();
    }

    @Override
    public NULL namedPattern(Expression.LeafExpression.Variable v, NULL aNull) {
        throw new UnsupportedOperationException();
    }

    @Override
    public NULL shortestPathPattern(InputPosition p, NULL aNull) {
        throw new UnsupportedOperationException();
    }

    @Override
    public NULL allShortestPathsPattern(InputPosition p, NULL aNull) {
        throw new UnsupportedOperationException();
    }

    @Override
    public NULL everyPathPattern(List<NULL> nodes, List<NULL> relationships) {
        throw new UnsupportedOperationException();
    }

    @Override
    public NULL nodePattern(
        InputPosition p,
        Expression.LeafExpression.Variable v,
        List<StringPos<InputPosition>> labels,
        Expression properties
    ) {
        throw new UnsupportedOperationException();
    }

    @Override
    public NULL relationshipPattern(
        InputPosition p,
        boolean left,
        boolean right,
        Expression.LeafExpression.Variable v,
        List<StringPos<InputPosition>> relTypes,
        NULL aNull,
        Expression properties,
        boolean legacyTypeSeparator
    ) {
        throw new UnsupportedOperationException();
    }

    @Override
    public NULL pathLength(InputPosition p, String minLength, String maxLength) {
        throw new UnsupportedOperationException();
    }

    @Override
    public NULL loadCsvClause(
        InputPosition p, boolean headers, Expression source, Expression.LeafExpression.Variable v, String fieldTerminator
    ) {
        throw new UnsupportedOperationException();
    }

    @Override
    public NULL foreachClause(
        InputPosition p, Expression.LeafExpression.Variable v, Expression list, List<NULL> nulls
    ) {
        throw new UnsupportedOperationException();
    }

    @Override
    public NULL subqueryClause(InputPosition p, NULL subquery) {
        throw new UnsupportedOperationException();
    }

    @Override
    public NULL useGraph(NULL aNull, NULL aNull2) {
        throw new UnsupportedOperationException();
    }

    @Override
    public NULL createRole(
        InputPosition p,
        boolean replace,
        Either<String, Expression> roleName,
        Either<String, Expression> fromRole,
        boolean ifNotExists
    ) {
        throw new UnsupportedOperationException();
    }

    @Override
    public NULL dropRole(InputPosition p, Either<String, Expression> roleName, boolean ifExists) {
        throw new UnsupportedOperationException();
    }

    @Override
    public NULL showRoles(
        InputPosition p,
        boolean withUsers,
        boolean showAll,
        NULL yieldExpr,
        NULL returnWithoutGraph,
        Expression where
    ) {
        throw new UnsupportedOperationException();
    }

    @Override
    public NULL yieldClause(
        InputPosition p,
        boolean returnAll,
        List<NULL> nulls,
        List<NULL> orderBy,
        Expression skip,
        Expression limit,
        Expression where
    ) {
        throw new UnsupportedOperationException();
    }

    @Override
    public NULL grantRoles(
        InputPosition p,
        List<Either<String, Expression>> roles,
        List<Either<String, Expression>> users
    ) {
        throw new UnsupportedOperationException();
    }

    @Override
    public NULL revokeRoles(
        InputPosition p,
        List<Either<String, Expression>> roles,
        List<Either<String, Expression>> users
    ) {
        throw new UnsupportedOperationException();
    }

    @Override
    public NULL createDatabase(
        InputPosition p,
        boolean replace,
        Either<String, Expression> databaseName,
        boolean ifNotExists,
        NULL aNull
    ) {
        throw new UnsupportedOperationException();
    }

    @Override
    public NULL dropDatabase(
        InputPosition p,
        Either<String, Expression> databaseName,
        boolean ifExists,
        boolean dumpData,
        NULL wait
    ) {
        throw new UnsupportedOperationException();
    }

    @Override
    public NULL showDatabase(InputPosition p, NULL aNull, NULL yieldExpr, NULL returnWithoutGraph, Expression where) {
        throw new UnsupportedOperationException();
    }

    @Override
    public NULL startDatabase(InputPosition p, Either<String, Expression> databaseName, NULL wait) {
        throw new UnsupportedOperationException();
    }

    @Override
    public NULL stopDatabase(InputPosition p, Either<String, Expression> databaseName, NULL wait) {
        throw new UnsupportedOperationException();
    }

    @Override
    public NULL databaseScope(InputPosition p, Either<String, Expression> databaseName, boolean isDefault) {
        throw new UnsupportedOperationException();
    }

    @Override
    public NULL wait(boolean wait, long seconds) {
        throw new UnsupportedOperationException();
    }
}
