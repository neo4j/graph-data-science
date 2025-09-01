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
package org.neo4j.gds.projection;

import org.junit.jupiter.api.Test;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.assertj.core.api.InstanceOfAssertFactories.type;

class CypherAggregationTest {

    @Test
    void catchesExceptionsAndRethrowsGqlCompliant() {
        var cypherAggregation = new CypherAggregation();
        var exception = assertThatThrownBy(() -> cypherAggregation.createReducer(null)).asInstanceOf(type(
            ProcedureException.class)).actual();
        var gqlCause = exception.gqlStatusObject().cause().orElse(exception.gqlStatusObject());
        assertThat(gqlCause.gqlStatus()).isEqualTo("53U00");
        assertThat(gqlCause.getMessage()).isEqualTo(
            "53U00: Execution of the function gds.graph.project() failed due to java.lang.NullPointerException: Cannot invoke \"org.neo4j.kernel.api.procedure.Context.graphDatabaseAPI()\" because \"ctx\" is null.");
    }
}
