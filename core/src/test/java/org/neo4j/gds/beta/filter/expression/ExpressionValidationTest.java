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

import org.immutables.value.Value;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.annotation.ValueClass;
import org.opencypher.v9_0.parser.javacc.ParseException;

import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;
import static org.neo4j.gds.beta.filter.expression.ValidationContext.Context.NODE;
import static org.neo4j.gds.beta.filter.expression.ValidationContext.Context.RELATIONSHIP;

class ExpressionValidationTest {

    @ParameterizedTest
    @ValueSource(strings = {"r", "foo"})
    void nodeVariable(String variableName) {
        var context = ImmutableTestValidationContext.builder().context(NODE).build();

        assertThatExceptionOfType(SemanticErrors.class)
            .isThrownBy(() -> ImmutableVariable.of(variableName).validate(context).validate())
            .withMessageContaining(formatWithLocale("Invalid variable `%s`. Only `n` is allowed for nodes", variableName));
    }

    @ParameterizedTest
    @ValueSource(strings = {"n", "foo"})
    void relationshipVariable(String variableName) {
        var context = ImmutableTestValidationContext.builder().context(RELATIONSHIP).build();

        assertThatExceptionOfType(SemanticErrors.class)
            .isThrownBy(() -> ImmutableVariable.of(variableName).validate(context).validate())
            .withMessageContaining(formatWithLocale("Invalid variable `%s`. Only `r` is allowed for relationships", variableName));
    }

    @Test
    void property() {
        var context = ImmutableTestValidationContext.builder().addAvailableProperties("bar").build();

        assertThatExceptionOfType(SemanticErrors.class)
            .isThrownBy(() -> ImmutableProperty.of(ImmutableVariable.of("n"), "baz").validate(context).validate())
            .withMessageContaining("Unknown property `baz`. Did you mean `bar`?");
    }

    @Test
    void hasLabelsOrTypes() {
        var context = ImmutableTestValidationContext.builder().addAvailableLabelsOrTypes("foo", "bar").build();

        assertThatExceptionOfType(SemanticErrors.class)
            .isThrownBy(() -> ImmutableHasLabelsOrTypes.of(ImmutableVariable.of("n"), List.of("foo", "baz")).validate(context).validate())
            .withMessageContaining("Unknown label `baz`. Did you mean `bar`?");
    }

    @Test
    void multipleErrors() throws ParseException {
        var expressionString = "n:Baz AND n.foo = 42";
        var expr = ExpressionParser.parse(expressionString);

        var context = ImmutableTestValidationContext.builder()
            .context(RELATIONSHIP)
            .addAvailableLabelsOrTypes("Foo", "Bar")
            .addAvailableProperties("bar", "foot")
            .build();

        assertThatExceptionOfType(SemanticErrors.class)
            .isThrownBy(() -> expr.validate(context).validate())
            .withMessageContaining("Only `r` is allowed")
            .withMessageContaining("Unknown relationship type `Baz`")
            .withMessageContaining("Unknown property `foo`");
    }

    @ValueClass
    interface TestValidationContext extends ValidationContext {
        @Override
        @Value.Default
        default Context context() {
            return NODE;
        }

        @Override
        @Value.Default
        default Set<String> availableProperties() {
            return Set.of();
        }

        @Override
        @Value.Default
        default Set<String> availableLabelsOrTypes() {
            return Set.of();
        }
    }

}
