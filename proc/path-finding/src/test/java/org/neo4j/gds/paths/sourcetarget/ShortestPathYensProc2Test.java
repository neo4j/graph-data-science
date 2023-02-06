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
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.RelationshipProjection;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.extension.Neo4jGraph;

class ShortestPathYensProc2Tes  extends  BaseProcTest{

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

  runQuery(GdsCypher.call("graph")
      .graphProject()
      .withAnyLabel()
      .withRelationshipType("TYPE", RelationshipProjection.builder().type("*").aggregation(Aggregation.SINGLE).build())
      .yields());
 }

 @Test
 void foo() {
  runQuery("" +
           "MATCH (source), (target) " +
           "WHERE id(source)=0 AND id(target)=5 " +
           "CALL gds.shortestPath.yens.stream(" +
           "  'graph', " +
           "  {sourceNode:source, targetNode:target, k:3} " +
           ") " +
           "YIELD sourceNode " +
           "RETURN *"
  );



 }
 @Test
 void bar() {
  runQuery("CALL gds.graph.project.cypher(\n" +
           "  'graphSingleType_NP',\n" +
           "  'MATCH (n) RETURN id(n) AS id',\n" +
           "  'MATCH (n)-[r]->(m) RETURN DISTINCT id(n) AS source, id(m) AS target'\n" +
           ")");

  runQuery("" +
           "MATCH (source), (target) " +
           "WHERE id(source)=0 AND id(target)=5 " +
           "CALL gds.shortestPath.yens.stream(" +
           "  'graphSingleType_NP', " +
           "  {sourceNode:source, targetNode:target, k:3} " +
           ") " +
           "YIELD sourceNode " +
           "RETURN *"
  );
 }
}

