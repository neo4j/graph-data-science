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
package org.neo4j.gds.beta.filter.expression;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.beta.filter.expression.Expression.LeafExpression.Variable;
import org.neo4j.gds.beta.filter.expression.Expression.UnaryExpression.HasNodeLabels;
import org.neo4j.gds.beta.filter.expression.Expression.UnaryExpression.HasRelationshipTypes;
import org.neo4j.gds.beta.filter.expression.Expression.UnaryExpression.Property;
import org.neo4j.gds.utils.StringJoining;
import org.opencypher.v9_0.parser.javacc.ParseException;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;
import static org.neo4j.gds.beta.filter.expression.ValidationContext.Context.NODE;
import static org.neo4j.gds.beta.filter.expression.ValidationContext.Context.RELATIONSHIP;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

class ExpressionValidationTest {

    @ParameterizedTest
    @ValueSource(strings = {"r", "foo"})
    void nodeVariable(String variableName) {
        var context = new ValidationContext(
            NODE,
            Set.of(),
            Set.of(),
            Map.of(),
            List.of()
        );

        assertThatExceptionOfType(SemanticErrors.class)
            .isThrownBy(() -> new Variable(variableName).validate(context).validate())
            .withMessageContaining(formatWithLocale(
                "Invalid variable `%s`. Only `n` is allowed for nodes",
                variableName
            ));
    }

    @ParameterizedTest
    @ValueSource(strings = {"n", "foo"})
    void relationshipVariable(String variableName) {
        var context = new ValidationContext(
            RELATIONSHIP,
            Set.of(),
            Set.of(),
            Map.of(),
            List.of()
        );

        assertThatExceptionOfType(SemanticErrors.class)
            .isThrownBy(() -> new Variable(variableName).validate(context).validate())
            .withMessageContaining(formatWithLocale(
                "Invalid variable `%s`. Only `r` is allowed for relationships",
                variableName
            ));
    }

    @Test
    void property() {
        var context = new  ValidationContext(
            NODE,
            Set.of(),
            Set.of(),
            Map.of("bar", ValueType.DOUBLE),
            List.of()
        );
        var expr = new Property(
            new Variable("n"),
            "baz",
            ValueType.DOUBLE
        );

        assertThatExceptionOfType(SemanticErrors.class)
            .isThrownBy(() -> expr.validate(context).validate())
            .withMessageContaining("Unknown property `baz`. Did you mean `bar`?");
    }

    @Test
    void hasLabels() {
        var context = new ValidationContext(
            NODE,
            Set.of(NodeLabel.of("foo"), NodeLabel.of("bar")),
            Set.of(),
            Map.of(),
            List.of()
        );

        var expr = new HasNodeLabels(
            new Variable("n"),
            List.of(NodeLabel.of("foo"), NodeLabel.of("baz"))
        );

        assertThatExceptionOfType(SemanticErrors.class)
            .isThrownBy(() -> expr.validate(context).validate())
            .withMessageContaining("Unknown label `baz`. Did you mean `bar`?");
    }

    @Test
    void hasTypes() {
        var context = new ValidationContext(
            NODE,
            Set.of(),
            Set.of(RelationshipType.of("foo"), RelationshipType.of("bar")),
            Map.of(),
            List.of()
        );

        var expr = new HasRelationshipTypes(
            new Variable("n"),
            List.of(RelationshipType.of("foo"), RelationshipType.of("baz"))
        );

        assertThatExceptionOfType(SemanticErrors.class)
            .isThrownBy(() -> expr.validate(context).validate())
            .withMessageContaining("Unknown label `baz`. Did you mean `bar`?");
    }

    @Test
    void multipleErrors() throws ParseException {
        var expressionString = "n:Baz AND n.foo = 42";
        var expr = ExpressionParser.parse(expressionString, Map.of());

        var context = new ValidationContext(
            RELATIONSHIP,
            Set.of(),
            Set.of(),
            Map.of(
                "bar", ValueType.DOUBLE,
                "foot", ValueType.DOUBLE
            ),
            List.of()
        );

        assertThatExceptionOfType(SemanticErrors.class)
            .isThrownBy(() -> expr.validate(context).validate())
            .withMessageContaining("Only `r` is allowed")
            .withMessageContaining("Unknown property `foo`");
    }

    @ParameterizedTest(name = "{0} ({1} vs {2})")
    @CsvSource(value = {
        "n.foo > 42,DOUBLE,LONG",
        "n.foo > 42.0,LONG,DOUBLE",

        "n.foo >= 42,DOUBLE,LONG",
        "n.foo >= 42.0,LONG,DOUBLE",

        "n.foo < 42,DOUBLE,LONG",
        "n.foo < 42.0,LONG,DOUBLE",

        "n.foo <= 42,DOUBLE,LONG",
        "n.foo <= 42.0,LONG,DOUBLE",

        "n.foo = 42,DOUBLE,LONG",
        "n.foo = 42.0,LONG,DOUBLE",

        "n.foo <> 42,DOUBLE,LONG",
        "n.foo <> 42.0,LONG,DOUBLE",
    })
    void incompatibleTypes(String exprString, ValueType lhsType, ValueType rhsType) throws ParseException {
        var context = new ValidationContext(
            NODE,
            Set.of(),
            Set.of(),
            Map.of("foo", lhsType),
            List.of()
        );

        var expr = ExpressionParser.parse(exprString, context.availableProperties());

        assertThatExceptionOfType(SemanticErrors.class)
            .isThrownBy(() -> expr.validate(context).validate())
            .withMessageContaining("Incompatible types")
            .withMessageContaining(lhsType.name())
            .withMessageContaining(rhsType.name())
            .withMessageContaining("in binary expression")
            .withMessageContaining(exprString);
    }

    @ParameterizedTest(name = "{0} ({1} vs {2})")
    @CsvSource(value = {
        "n.foo > 42,DOUBLE,LONG,literal to `42.0`",
        "n.foo > 42.0,LONG,DOUBLE,literal to `42`",
    })
    void incompatibleTypesWithLiteralHint(
        String exprString,
        ValueType lhsType,
        ValueType rhsType,
        String literalHint
    ) throws ParseException {
        var context = new ValidationContext(
            NODE,
            Set.of(),
            Set.of(),
            Map.of("foo", lhsType),
            List.of()
        );

        var expr = ExpressionParser.parse(exprString, context.availableProperties());

        assertThatExceptionOfType(SemanticErrors.class)
            .isThrownBy(() -> expr.validate(context).validate())
            .withMessageContaining("Incompatible types")
            .withMessageContaining(lhsType.name())
            .withMessageContaining(rhsType.name())
            .withMessageContaining("in binary expression")
            .withMessageContaining(exprString)
            .withMessageContaining(literalHint);
    }

    @ParameterizedTest(name = "{0} {1}")
    @CsvSource(value = {
        "n.foo,STRING",
        "n.foo,UNTYPED_ARRAY"})
    void unsupportedTypes(String exprString, ValueType valueType) throws ParseException {
        var context = new ValidationContext(
            NODE,
            Set.of(),
            Set.of(),
            Map.of("foo", valueType),
            List.of()
        );

        var expr = ExpressionParser.parse(exprString, context.availableProperties());

        assertThatExceptionOfType(SemanticErrors.class)
            .isThrownBy(() -> expr.validate(context).validate())
            .withMessageContaining("Unsupported property type `%s` for expression", valueType.name())
            .withMessageContaining(exprString)
            .withMessageContaining(StringJoining.join(List.of(ValueType.LONG.name(), ValueType.DOUBLE.name())));
    }
}
