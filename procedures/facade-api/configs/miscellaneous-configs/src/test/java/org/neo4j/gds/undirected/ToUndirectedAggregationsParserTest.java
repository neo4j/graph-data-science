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
package org.neo4j.gds.undirected;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.core.Aggregation;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ToUndirectedAggregationsParserTest {

    @Test
    void shouldParseAggregationObject(){
        Aggregation max = Aggregation.MAX;
        var aggregation = ToUndirectedAggregationsParser.parse(max);
        assertThat(aggregation).isInstanceOf(ToUndirectedAggregations.GlobalAggregation.class);
        assertThat(aggregation.globalAggregation().orElseThrow()).isEqualTo(max);
    }
    @Test
    void shouldParseString(){
        Aggregation max = Aggregation.MAX;
        var aggregation = ToUndirectedAggregationsParser.parse("MAX");
        assertThat(aggregation).isInstanceOf(ToUndirectedAggregations.GlobalAggregation.class);
        assertThat(aggregation.globalAggregation().orElseThrow()).isEqualTo(max);
    }

    @Test
    void shouldParseMap(){
        Aggregation single = Aggregation.SINGLE;
        Aggregation sum = Aggregation.SUM;

        var aggregation = ToUndirectedAggregationsParser.parse(
            Map.of(
                    "foo","SINGLE",
                   "bar","SUM"
            )
        );
        assertThat(aggregation).isInstanceOf(ToUndirectedAggregations.AggregationPerProperty.class);
        assertThat(aggregation.localAggregation("foo")).isEqualTo(single);
        assertThat(aggregation.localAggregation("bar")).isEqualTo(sum);
    }

    @Test
    void shouldCreateStringForGlobalCorrectly(){
        var aggregation = ToUndirectedAggregationsParser.parse("MAX");
        assertThat(ToUndirectedAggregationsParser.toString(aggregation)).isEqualTo("MAX");
    }

    @Test
    void shouldCreateStringPerPropertyCorrectly(){
        var aggregation = ToUndirectedAggregationsParser.parse(
            Map.of(
                "foo","SINGLE",
                "bar","SUM"
            )
        );        assertThat(ToUndirectedAggregationsParser.toString(aggregation)).isEqualTo("{bar=SUM, foo=SINGLE}");
    }

}
