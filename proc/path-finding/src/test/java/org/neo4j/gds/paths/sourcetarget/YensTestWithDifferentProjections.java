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
package org.neo4j.gds.paths.sourcetarget;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.extension.Neo4jGraph;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class YensTestWithDifferentProjections extends BaseProcTest {

 @Neo4jGraph
 private static final String DB_CYPHER =
     "CREATE (a:CITY), " +
     "(b:CITY), " +
     "(c:CITY), " +
     "(d:CITY), " +
     "(e:CITY), " +
     "(f:CITY), " +
     "(a)-[:ROAD]->(b), " +
     "(a)-[:ROAD]->(b), " +
     "(b)-[:ROAD]->(c), " +
     "(b)-[:ROAD]->(d), " +
     "(c)-[:ROAD]->(f), " +
     "(d)-[:ROAD]->(e), " +
     "(e)-[:ROAD]->(c), " +
     "(e)-[:ROAD]->(f), " +
     "(a)-[:PATH]->(b), " +
     "(d)-[:PATH]->(e), " +
     "(d)-[:PATH]->(e)";

 @BeforeEach
 void setup() throws Exception {
  registerProcedures(
      ShortestPathYensStreamProc.class,
      GraphProjectProc.class
  );
 }


 @ParameterizedTest
 @ValueSource(strings = {
     "CALL gds.graph.project('g', '*', {TYPE: {type: '*', aggregation: 'SINGLE'}})",
     "CALL gds.graph.project.cypher('g', 'MATCH (n) RETURN id(n) AS id', 'MATCH (n)-[r]->(m) RETURN DISTINCT id(n) AS source, id(m) AS target')"
 })
 void shouldWorkWithDifferentProjections(String projectionQuery) {

  runQuery(projectionQuery);
  String yensQuery = "MATCH (source), (target) " +
                     "WHERE id(source)=0 AND id(target)=5 " +
                     "CALL gds.shortestPath.yens.stream(" +
                     "  'g', " +
                     "  {sourceNode:source, targetNode:target, k:3} " +
                     ") " +
                     "YIELD  nodeIds RETURN nodeIds ";

  Collection<long[]> encounteredPaths = new HashSet<>();
  runQuery(yensQuery, result -> {
   assertThat(result.columns()).containsExactlyInAnyOrder("nodeIds");

   while (result.hasNext()) {
    var next = result.next();
    var currentPath = (List<Long>) next.get("nodeIds");
    long[] pathToArray = currentPath.stream().mapToLong(l -> l).toArray();
    encounteredPaths.add(pathToArray);
   }

   return true;
  });

  assertThat(encounteredPaths).containsExactlyInAnyOrder(
      new long[]{0l, 1l, 3l, 4l, 2l, 5l},
      new long[]{0l, 1l, 3l, 4l, 5l},
      new long[]{0l, 1l, 2l, 5l}
  );
 }

}

