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
package org.neo4j.gds.core.loading;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.core.Aggregation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.core.loading.AdjacencyPreAggregation.IGNORE_VALUE;

class AdjacencyPreAggregationTest {

    @Test
    void testAggregation() {
        var targets = new long[]{3, 1, 3, 2, 2, 1};
        var properties = new long[3][targets.length];

        properties[0] = new long[]{1, 1, 1, 1, 1, 1};
        properties[1] = new long[]{1, 2, 3, 4, 5, 6};
        properties[2] = new long[]{1, 2, 3, 4, 5, 6};

        var aggregations = new Aggregation[]{Aggregation.SUM, Aggregation.MAX, Aggregation.SINGLE};

        AdjacencyPreAggregation.preAggregate(targets, properties, 0, targets.length, aggregations);

        assertThat(targets).containsExactly(3, 1, IGNORE_VALUE, 2, IGNORE_VALUE, IGNORE_VALUE);
        assertThat(properties[0]).containsExactly(2, 2, 1, 2, 1, 1);
        assertThat(properties[1]).containsExactly(3, 6, 3, 5, 5, 6);
        assertThat(properties[2]).containsExactly(1, 2, 3, 4, 5, 6);
    }

    @Test
    void testAggregationWithRange() {
        var targets = new long[]{3, 1, 3, 2, 2, 1};
        var properties = new long[3][targets.length];

        properties[0] = new long[]{1, 1, 1, 1, 1, 1};
        properties[1] = new long[]{1, 2, 3, 4, 5, 6};
        properties[2] = new long[]{1, 2, 3, 4, 5, 6};

        var aggregations = new Aggregation[]{Aggregation.SUM, Aggregation.MAX, Aggregation.SINGLE};

        AdjacencyPreAggregation.preAggregate(targets, properties, 1, 5, aggregations);

        assertThat(targets).containsExactly(3, 1, 3, 2, IGNORE_VALUE, 1);
        assertThat(properties[0]).containsExactly(1, 1, 1, 2, 1, 1);
        assertThat(properties[1]).containsExactly(1, 2, 3, 5, 5, 6);
        assertThat(properties[2]).containsExactly(1, 2, 3, 4, 5, 6);
    }


}
