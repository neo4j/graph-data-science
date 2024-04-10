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
package org.neo4j.gds.influenceMaximization;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.api.schema.Direction;
import org.neo4j.gds.beta.generator.RandomGraphGenerator;
import org.neo4j.gds.beta.generator.RelationshipDistribution;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import static org.assertj.core.api.Assertions.assertThat;

 class CelfTest {

     @ParameterizedTest
     @ValueSource(ints = {2, 7})
     void shouldNotReturnNegativeGains(int seedSize) {
         var graph = RandomGraphGenerator
             .builder()
             .averageDegree(5)
             .relationshipDistribution(RelationshipDistribution.POWER_LAW)
             .direction(Direction.DIRECTED)
             .nodeCount(60)
             .seed(42)
             .build()
             .generate();

         var parameters = new CELFParameters(
             seedSize,
             0.1,
             3,
             new Concurrency(1),
             10L,
             5
         );

         var celf = new CELF(
             graph,
             parameters,
             DefaultPool.INSTANCE,
             ProgressTracker.NULL_TRACKER
         ).compute().seedSetNodes();

         for (var a : celf) {
             assertThat(a.value).isNotNegative();
         }
     }
}
