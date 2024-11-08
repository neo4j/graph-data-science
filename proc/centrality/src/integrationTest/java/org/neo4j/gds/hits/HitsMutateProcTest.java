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
package org.neo4j.gds.hits;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.Neo4jGraph;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.LONG;

class HitsMutateProcTest extends BaseProcTest {

   @Neo4jGraph
   private static final String DB_CYPHER =
       "CREATE" +
           "  (a:Node {name: 'a'})" +
           ", (b:Node {name: 'b'})" +
           ", (c:Node {name: 'c'})" +
           ", (a)-[:REL]->(b)" +
           ", (b)-[:REL]->(c)" ;

   @Inject
   private IdFunction idFunction;

   @BeforeEach
   void setup() throws Exception {
       registerProcedures(
           HitsMutateProc.class,
           GraphProjectProc.class
       );

       runQuery(
           GdsCypher.call(DEFAULT_GRAPH_NAME)
               .graphProject()
               .loadEverything(Orientation.NATURAL)
               .yields()
       );

   }

       @Test
       void testMutate() {

           double authValue =0.7071067811865475;
           double hubValue = 0.7071067811865476;

           var query = GdsCypher.call(DEFAULT_GRAPH_NAME)
               .algo("gds.hits")
               .mutateMode()
               .addParameter("hitsIterations",1)
               .addParameter("mutateProperty","foo")
               .yields();


           var rowCount = runQueryWithRowConsumer(query, (resultRow) -> {
               assertThat(resultRow.getNumber("ranIterations")).asInstanceOf(LONG).isEqualTo(4L);
               assertThat(resultRow.getNumber("nodePropertiesWritten")).asInstanceOf(LONG).isEqualTo(6L);

           });
           assertThat(rowCount).isEqualTo(1);

           var actualGraph = GraphStoreCatalog.get(getUsername(), DatabaseId.of(db.databaseName()), "graph")
               .graphStore()
               .getUnion();

           var hubValues= actualGraph.nodeProperties("hubfoo");
           var authValues= actualGraph.nodeProperties("authfoo");

           for (long u=0;u<3;++u){
               double hubOfu = hubValues.doubleValue(u);
               double authOfu = authValues.doubleValue(u);

              assertThat(hubOfu)
                  .satisfiesAnyOf(
                      v -> assertThat(v).isCloseTo(0, Offset.offset(1e-6)),
                      v -> assertThat(v).isCloseTo(hubValue, Offset.offset(1e-6)));

               assertThat(authOfu)
                   .satisfiesAnyOf(
                       v -> assertThat(v).isCloseTo(0, Offset.offset(1e-6)),
                       v -> assertThat(v).isCloseTo(authValue, Offset.offset(1e-6)));
           }
       }

}
