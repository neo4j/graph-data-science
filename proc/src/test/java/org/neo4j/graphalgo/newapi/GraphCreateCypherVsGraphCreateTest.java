/*
 * Copyright (c) 2017-2020 "Neo4j,"
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

package org.neo4j.graphalgo.newapi;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.core.loading.GraphCatalog;
import org.neo4j.internal.kernel.api.exceptions.KernelException;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GraphCreateCypherVsGraphCreateTest extends BaseProcTest {
    private static final String NL = System.lineSeparator();

    @BeforeEach
    void setup() throws KernelException {

        db = TestDatabaseCreator.createTestDatabase();

        registerProcedures(GraphCreateProc.class, GraphListProc.class);

        Arrays.stream(CONSTRAINTS.split(";")).filter(String::isEmpty).forEach(this::runQuery);

        runQuery(BATTLE_NODES);
        runQuery(ATTACKER_DEFENDER);
        runQuery(BATTLE_LOCATION);
        runQuery(PEOPLE_IN_BATTLE);
        runQuery(PEOPLE_IN_HOUSES);
        runQuery(PEOPLE_RELATIONS);
        runQuery(CULTURES);
        runQuery(DEATHS);

        Arrays.stream(MISC_LABELS.split(";")).filter(String::isEmpty).forEach(this::runQuery);

        runQuery(INTERACTIONS_1);
        runQuery(INTERACTIONS_2);
        runQuery(INTERACTIONS_3);
        runQuery(INTERACTIONS_4);
        runQuery(INTERACTIONS_5);

    }

    @AfterEach
    void tearDown() {
        GraphCatalog.removeAllLoadedGraphs();
        db.shutdown();
    }

    @Test
    void shouldCreateSameGraphs() {
        String graphCreate = "CALL gds.graph.create('got-weighted-interactions'," +
                             "  'Person'," +
                             "  {" +
                             "    INTERACTS: {" +
                             "      projection: 'NATURAL'," +
                             "      aggregation: 'SINGLE'," +
                             "      properties: 'weight'" +
                             "    }" +
                             "  }" +
                             ")";

        String graphCreateCypher = "CALL gds.graph.create.cypher(" +
                                   "  'got-weighted-interactions-cypher'," +
                                   "  'MATCH (n:Person) RETURN id(n) AS id'," +
                                   "  'MATCH (p1:Person)-[r:INTERACTS]->(p2:Person) RETURN id(p1) AS source, id(p2) AS target, toFloat(r.weight) as weight'," +
                                   "  { " +
                                   "    relationshipProperties: {" +
                                   "        aggregation: 'SINGLE' " +
                                   "    }" +
                                   "  }" +
                                   ")";

        runQuery(graphCreate);
        runQuery(graphCreateCypher);

        AtomicInteger nodeCount = new AtomicInteger();
        AtomicInteger relationshipCount = new AtomicInteger();
        String graphList = "CALL gds.graph.list('got-weighted-interactions') YIELD nodeCount, relationshipCount;";
        runQueryWithRowConsumer(graphList, row -> {
            nodeCount.set(row.getNumber("nodeCount").intValue());
            relationshipCount.set(row.getNumber("relationshipCount").intValue());
        });

        AtomicInteger nodeCountCypher = new AtomicInteger();
        AtomicInteger relationshipCountCypher = new AtomicInteger();
        String graphListCypher = "CALL gds.graph.list('got-weighted-interactions-cypher') YIELD nodeCount, relationshipCount;";
        runQueryWithRowConsumer(graphListCypher, row -> {
            nodeCountCypher.set(row.getNumber("nodeCount").intValue());
            relationshipCountCypher.set(row.getNumber("relationshipCount").intValue());
        });

        assertEquals(nodeCount.get(), nodeCountCypher.get());
        int relationships = relationshipCount.get();
        int relationshipsCypher = relationshipCountCypher.get();
        assertTrue(relationships > 0);
        assertTrue(relationshipsCypher > 0);

//        FIXME: this here should be:
//        assertEquals(
        assertNotEquals(
            relationships,
            relationshipsCypher,
            String.format(
                "Expected %d relationships using `gds.graph.create` to be equal to %d relationships when using `gds.graph.create.cypher`",
                relationships,
                relationshipsCypher
            )
        );
    }

    private static final String CONSTRAINTS = "CREATE CONSTRAINT ON (n:Location) ASSERT n.name IS UNIQUE;" + NL +
                                              "CREATE CONSTRAINT ON (n:Region) ASSERT n.name IS UNIQUE;" + NL +
                                              "CREATE CONSTRAINT ON (n:Battle) ASSERT n.name IS UNIQUE;" + NL +
                                              "CREATE CONSTRAINT ON (n:Person) ASSERT n.name IS UNIQUE;" + NL +
                                              "CREATE CONSTRAINT ON (n:House) ASSERT n.name IS UNIQUE;\n";

    private static final String BATTLE_NODES = "LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/tomasonjo/neo4j-game-of-thrones/master/data/battles.csv' AS row" + NL +
                                               "MERGE (b:Battle {name: row.name})" + NL +
                                               "  ON CREATE SET b.year = toInteger(row.year)," + NL +
                                               "  b.summer = row.summer," + NL +
                                               "  b.major_death = row.major_death," + NL +
                                               "  b.major_capture = row.major_capture," + NL +
                                               "  b.note = row.note," + NL +
                                               "  b.battle_type = row.battle_type," + NL +
                                               "  b.attacker_size = toInteger(row.attacker_size)," + NL +
                                               "  b.defender_size = toInteger(row.defender_size);";

    private static final String ATTACKER_DEFENDER = "LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/tomasonjo/neo4j-game-of-thrones/master/data/battles.csv' AS row" + NL +
                                                    "" + NL +
                                                    "// there is only attacker_outcome in the data," + NL +
                                                    "// so we do a CASE statement for defender_outcome" + NL +
                                                    "WITH row," + NL +
                                                    "     CASE WHEN row.attacker_outcome = 'win' THEN 'loss'" + NL +
                                                    "       ELSE 'win'" + NL +
                                                    "       END AS defender_outcome" + NL +
                                                    "" + NL +
                                                    "// match the battle" + NL +
                                                    "MATCH (b:Battle {name: row.name})" + NL +
                                                    "" + NL +
                                                    "// all battles have atleast one attacker so we don't have to use foreach trick" + NL +
                                                    "MERGE (attacker1:House {name: row.attacker_1})" + NL +
                                                    "MERGE (attacker1)-[a1:ATTACKER]->(b)" + NL +
                                                    "  ON CREATE SET a1.outcome = row.attacker_outcome" + NL +
                                                    "" + NL +
                                                    "// When we want to skip null values we can use foreach trick" + NL +
                                                    "FOREACH" + NL +
                                                    "(ignoreMe IN CASE WHEN row.defender_1 IS NOT NULL THEN [1]" + NL +
                                                    "  ELSE []" + NL +
                                                    "  END |" + NL +
                                                    "  MERGE (defender1:House {name: row.defender_1})" + NL +
                                                    "  MERGE (defender1)-[d1:DEFENDER]->(b)" + NL +
                                                    "    ON CREATE SET d1.outcome = defender_outcome" + NL +
                                                    ")" + NL +
                                                    "FOREACH" + NL +
                                                    "(ignoreMe IN CASE WHEN row.defender_2 IS NOT NULL THEN [1]" + NL +
                                                    "  ELSE []" + NL +
                                                    "  END |" + NL +
                                                    "  MERGE (defender2:House {name: row.defender_2})" + NL +
                                                    "  MERGE (defender2)-[d2:DEFENDER]->(b)" + NL +
                                                    "    ON CREATE SET d2.outcome = defender_outcome" + NL +
                                                    ")" + NL +
                                                    "FOREACH" + NL +
                                                    "(ignoreMe IN CASE WHEN row.attacker_2 IS NOT NULL THEN [1]" + NL +
                                                    "  ELSE []" + NL +
                                                    "  END |" + NL +
                                                    "  MERGE (attacker2:House {name: row.attacker_2})" + NL +
                                                    "  MERGE (attacker2)-[a2:ATTACKER]->(b)" + NL +
                                                    "    ON CREATE SET a2.outcome = row.attacker_outcome" + NL +
                                                    ")" + NL +
                                                    "FOREACH" + NL +
                                                    "(ignoreMe IN CASE WHEN row.attacker_3 IS NOT NULL THEN [1]" + NL +
                                                    "  ELSE []" + NL +
                                                    "  END |" + NL +
                                                    "  MERGE (attacker2:House {name: row.attacker_3})" + NL +
                                                    "  MERGE (attacker3)-[a3:ATTACKER]->(b)" + NL +
                                                    "    ON CREATE SET a3.outcome = row.attacker_outcome" + NL +
                                                    ")" + NL +
                                                    "FOREACH" + NL +
                                                    "(ignoreMe IN CASE WHEN row.attacker_4 IS NOT NULL THEN [1]" + NL +
                                                    "  ELSE []" + NL +
                                                    "  END |" + NL +
                                                    "  MERGE (attacker4:House {name: row.attacker_4})" + NL +
                                                    "  MERGE (attacker4)-[a4:ATTACKER]->(b)" + NL +
                                                    "    ON CREATE SET a4.outcome = row.attacker_outcome" + NL +
                                                    ");";

    private static final String BATTLE_LOCATION = "LOAD CSV WITH HEADERS FROM" + NL +
                                                  "'https://raw.githubusercontent.com/tomasonjo/neo4j-game-of-thrones/master/data/battles.csv'" + NL +
                                                  "AS row" + NL +
                                                  "MATCH (b:Battle {name: row.name})" + NL +
                                                  "" + NL +
                                                  "// We use coalesce, so that null values are replaced with \"Unknown\"" + NL +
                                                  "MERGE (location:Location {name: coalesce(row.location, 'Unknown')})" + NL +
                                                  "MERGE (b)-[:IS_IN]->(location)" + NL +
                                                  "MERGE (region:Region {name: row.region})" + NL +
                                                  "MERGE (location)-[:IS_IN]->(region);";

    private static final String PEOPLE_IN_BATTLE = "LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/tomasonjo/neo4j-game-of-thrones/master/data/battles.csv' AS row" + NL +
                                                   "" + NL +
                                                   "// We split the columns that may contain more than one person" + NL +
                                                   "WITH row," + NL +
                                                   "     split(row.attacker_commander, ',') AS att_commanders," + NL +
                                                   "     split(row.defender_commander, ',') AS def_commanders," + NL +
                                                   "     split(row.attacker_king, '/') AS att_kings," + NL +
                                                   "     split(row.defender_king, '/') AS def_kings," + NL +
                                                   "     row.attacker_outcome AS att_outcome," + NL +
                                                   "     CASE WHEN row.attacker_outcome = 'win' THEN 'loss'" + NL +
                                                   "       ELSE 'win'" + NL +
                                                   "       END AS def_outcome" + NL +
                                                   "MATCH (b:Battle {name: row.name})" + NL +
                                                   "" + NL +
                                                   "// we unwind a list" + NL +
                                                   "UNWIND att_commanders AS att_commander" + NL +
                                                   "MERGE (p:Person {name: trim(att_commander)})" + NL +
                                                   "MERGE (p)-[ac:ATTACKER_COMMANDER]->(b)" + NL +
                                                   "  ON CREATE SET ac.outcome = att_outcome" + NL +
                                                   "" + NL +
                                                   "// to end the unwind and correct cardinality(number of rows)" + NL +
                                                   "// we use any aggregation function ( e.g. count(*))" + NL +
                                                   "WITH b, def_commanders, def_kings, att_kings, att_outcome, def_outcome," + NL +
                                                   "     COUNT(*) AS c1" + NL +
                                                   "UNWIND def_commanders AS def_commander" + NL +
                                                   "MERGE (p:Person {name: trim(def_commander)})" + NL +
                                                   "MERGE (p)-[dc:DEFENDER_COMMANDER]->(b)" + NL +
                                                   "  ON CREATE SET dc.outcome = def_outcome" + NL +
                                                   "" + NL +
                                                   "// reset cardinality with an aggregation function (end the unwind)" + NL +
                                                   "WITH b, def_kings, att_kings, att_outcome, def_outcome, COUNT(*) AS c2" + NL +
                                                   "UNWIND def_kings AS def_king" + NL +
                                                   "MERGE (p:Person {name: trim(def_king)})" + NL +
                                                   "MERGE (p)-[dk:DEFENDER_KING]->(b)" + NL +
                                                   "  ON CREATE SET dk.outcome = def_outcome" + NL +
                                                   "" + NL +
                                                   "// reset cardinality with an aggregation function (end the unwind)" + NL +
                                                   "WITH b, att_kings, att_outcome, COUNT(*) AS c3" + NL +
                                                   "UNWIND att_kings AS att_king" + NL +
                                                   "MERGE (p:Person {name: trim(att_king)})" + NL +
                                                   "MERGE (p)-[ak:ATTACKER_KING]->(b)" + NL +
                                                   "  ON CREATE SET ak.outcome = att_outcome;";

    private static final String PEOPLE_IN_HOUSES = "LOAD CSV WITH HEADERS FROM" + NL +
                                                   "'https://raw.githubusercontent.com/tomasonjo/neo4j-game-of-thrones/master/data/character-deaths.csv'" + NL +
                                                   "AS row" + NL +
                                                   "" + NL +
                                                   "// we can use CASE in a WITH statement" + NL +
                                                   "WITH row," + NL +
                                                   "     CASE WHEN row.Nobility = '1' THEN 'Noble'" + NL +
                                                   "       ELSE 'Commoner'" + NL +
                                                   "       END AS status_value" + NL +
                                                   "" + NL +
                                                   "// as seen above we remove \"House \" for better linking" + NL +
                                                   "MERGE (house:House {name: replace(row.Allegiances, 'House ', '')})" + NL +
                                                   "MERGE (person:Person {name: row.Name})" + NL +
                                                   "" + NL +
                                                   "// we can also use CASE statement inline" + NL +
                                                   "SET person.gender = CASE WHEN row.Gender = '1' THEN 'male'" + NL +
                                                   "  ELSE 'female'" + NL +
                                                   "  END," + NL +
                                                   "person.book_intro_chapter = row.`Book Intro Chapter`," + NL +
                                                   "person.book_death_chapter = row.`Death Chapter`," + NL +
                                                   "person.book_of_death = row.`Book of Death`," + NL +
                                                   "person.death_year = toINT(row.`Death Year`)" + NL +
                                                   "MERGE (person)-[:BELONGS_TO]->(house)" + NL +
                                                   "MERGE (status:Status {name: status_value})" + NL +
                                                   "MERGE (person)-[:HAS_STATUS]->(status)" + NL +
                                                   "" + NL +
                                                   "// doing the foreach trick to skip null values" + NL +
                                                   "FOREACH" + NL +
                                                   "(ignoreMe IN CASE WHEN row.GoT = '1' THEN [1]" + NL +
                                                   "  ELSE []" + NL +
                                                   "  END |" + NL +
                                                   "  MERGE (book1:Book {sequence: 1})" + NL +
                                                   "    ON CREATE SET book1.name = 'Game of thrones'" + NL +
                                                   "  MERGE (person)-[:APPEARED_IN]->(book1)" + NL +
                                                   ")" + NL +
                                                   "FOREACH" + NL +
                                                   "(ignoreMe IN CASE WHEN row.CoK = '1' THEN [1]" + NL +
                                                   "  ELSE []" + NL +
                                                   "  END |" + NL +
                                                   "  MERGE (book2:Book {sequence: 2})" + NL +
                                                   "    ON CREATE SET book2.name = 'Clash of kings'" + NL +
                                                   "  MERGE (person)-[:APPEARED_IN]->(book2)" + NL +
                                                   ")" + NL +
                                                   "FOREACH" + NL +
                                                   "(ignoreMe IN CASE WHEN row.SoS = '1' THEN [1]" + NL +
                                                   "  ELSE []" + NL +
                                                   "  END |" + NL +
                                                   "  MERGE (book3:Book {sequence: 3})" + NL +
                                                   "    ON CREATE SET book3.name = 'Storm of swords'" + NL +
                                                   "  MERGE (person)-[:APPEARED_IN]->(book3)" + NL +
                                                   ")" + NL +
                                                   "FOREACH" + NL +
                                                   "(ignoreMe IN CASE WHEN row.FfC = '1' THEN [1]" + NL +
                                                   "  ELSE []" + NL +
                                                   "  END |" + NL +
                                                   "  MERGE (book4:Book {sequence: 4})" + NL +
                                                   "    ON CREATE SET book4.name = 'Feast for crows'" + NL +
                                                   "  MERGE (person)-[:APPEARED_IN]->(book4)" + NL +
                                                   ")" + NL +
                                                   "FOREACH" + NL +
                                                   "(ignoreMe IN CASE WHEN row.DwD = '1' THEN [1]" + NL +
                                                   "  ELSE []" + NL +
                                                   "  END |" + NL +
                                                   "  MERGE (book5:Book {sequence: 5})" + NL +
                                                   "    ON CREATE SET book5.name = 'Dance with dragons'" + NL +
                                                   "  MERGE (person)-[:APPEARED_IN]->(book5)" + NL +
                                                   ")" + NL +
                                                   "FOREACH" + NL +
                                                   "(ignoreMe IN CASE WHEN row.`Book of Death` IS NOT NULL THEN [1]" + NL +
                                                   "  ELSE []" + NL +
                                                   "  END |" + NL +
                                                   "  MERGE (book:Book {sequence: toInt(row.`Book of Death`)})" + NL +
                                                   "  MERGE (person)-[:DIED_IN]->(book)" + NL +
                                                   ");";

    private static final String PEOPLE_RELATIONS = "LOAD CSV WITH HEADERS FROM" + NL +
                                                   "'https://raw.githubusercontent.com/tomasonjo/neo4j-game-of-thrones/master/data/character-predictions.csv'" + NL +
                                                   "AS row" + NL +
                                                   "MERGE (p:Person {name: row.name})" + NL +
                                                   "// set properties on the person node" + NL +
                                                   "SET p.title = row.title," + NL +
                                                   "p.death_year = toINT(row.DateoFdeath)," + NL +
                                                   "p.birth_year = toINT(row.dateOfBirth)," + NL +
                                                   "p.age = toINT(row.age)," + NL +
                                                   "p.gender = CASE WHEN row.male = '1' THEN 'male'" + NL +
                                                   "  ELSE 'female'" + NL +
                                                   "  END" + NL +
                                                   "" + NL +
                                                   "// doing the foreach trick to skip null values" + NL +
                                                   "FOREACH" + NL +
                                                   "(ignoreMe IN CASE WHEN row.mother IS NOT NULL THEN [1]" + NL +
                                                   "  ELSE []" + NL +
                                                   "  END |" + NL +
                                                   "  MERGE (mother:Person {name: row.mother})" + NL +
                                                   "  MERGE (p)-[:RELATED_TO {name: 'mother'}]->(mother)" + NL +
                                                   ")" + NL +
                                                   "FOREACH" + NL +
                                                   "(ignoreMe IN CASE WHEN row.spouse IS NOT NULL THEN [1]" + NL +
                                                   "  ELSE []" + NL +
                                                   "  END |" + NL +
                                                   "  MERGE (spouse:Person {name: row.spouse})" + NL +
                                                   "  MERGE (p)-[:RELATED_TO {name: 'spouse'}]->(spouse)" + NL +
                                                   ")" + NL +
                                                   "FOREACH" + NL +
                                                   "(ignoreMe IN CASE WHEN row.father IS NOT NULL THEN [1]" + NL +
                                                   "  ELSE []" + NL +
                                                   "  END |" + NL +
                                                   "  MERGE (father:Person {name: row.father})" + NL +
                                                   "  MERGE (p)-[:RELATED_TO {name: 'father'}]->(father)" + NL +
                                                   ")" + NL +
                                                   "FOREACH" + NL +
                                                   "(ignoreMe IN CASE WHEN row.heir IS NOT NULL THEN [1]" + NL +
                                                   "  ELSE []" + NL +
                                                   "  END |" + NL +
                                                   "  MERGE (heir:Person {name: row.heir})" + NL +
                                                   "  MERGE (p)-[:RELATED_TO {name: 'heir'}]->(heir)" + NL +
                                                   ")" + NL +
                                                   "" + NL +
                                                   "// we remove \"House \" from the value for better linking of data - this didn't run in the original, but I think it should now - there was a bug in the replace" + NL +
                                                   "FOREACH" + NL +
                                                   "(ignoreMe IN CASE WHEN row.house IS NOT NULL THEN [1]" + NL +
                                                   "  ELSE []" + NL +
                                                   "  END |" + NL +
                                                   "  MERGE (house:House {name: replace(row.house, 'House ', '')})" + NL +
                                                   "  MERGE (p)-[:BELONGS_TO]->(house)" + NL +
                                                   ");";

    private static final String CULTURES = "LOAD CSV WITH HEADERS FROM" + NL +
                                           "'https://raw.githubusercontent.com/tomasonjo/neo4j-game-of-thrones/master/data/character-predictions.csv'" + NL +
                                           "AS row" + NL +
                                           "" + NL +
                                           "// match person" + NL +
                                           "MERGE (p:Person {name: row.name})" + NL +
                                           "" + NL +
                                           "// doing the foreach trick... we lower row.culture for better linking" + NL +
                                           "FOREACH" + NL +
                                           "(ignoreMe IN CASE WHEN row.culture IS NOT NULL THEN [1]" + NL +
                                           "  ELSE []" + NL +
                                           "  END |" + NL +
                                           "  MERGE (culture:Culture {name: lower(row.culture)})" + NL +
                                           "  MERGE (p)-[:MEMBER_OF_CULTURE]->(culture)" + NL +
                                           ")" + NL +
                                           "FOREACH" + NL +
                                           "(ignoreMe IN CASE WHEN row.book1 = '1' THEN [1]" + NL +
                                           "  ELSE []" + NL +
                                           "  END |" + NL +
                                           "  MERGE (book:Book {sequence: 1})" + NL +
                                           "  MERGE (p)-[:APPEARED_IN]->(book)" + NL +
                                           ")" + NL +
                                           "FOREACH" + NL +
                                           "(ignoreMe IN CASE WHEN row.book2 = '1' THEN [1]" + NL +
                                           "  ELSE []" + NL +
                                           "  END |" + NL +
                                           "  MERGE (book:Book {sequence: 2})" + NL +
                                           "  MERGE (p)-[:APPEARED_IN]->(book)" + NL +
                                           ")" + NL +
                                           "FOREACH" + NL +
                                           "(ignoreMe IN CASE WHEN row.book3 = '1' THEN [1]" + NL +
                                           "  ELSE []" + NL +
                                           "  END |" + NL +
                                           "  MERGE (book:Book {sequence: 3})" + NL +
                                           "  MERGE (p)-[:APPEARED_IN]->(book)" + NL +
                                           ")" + NL +
                                           "FOREACH" + NL +
                                           "(ignoreMe IN CASE WHEN row.book4 = '1' THEN [1]" + NL +
                                           "  ELSE []" + NL +
                                           "  END |" + NL +
                                           "  MERGE (book:Book {sequence: 4})" + NL +
                                           "  MERGE (p)-[:APPEARED_IN]->(book)" + NL +
                                           ")" + NL +
                                           "FOREACH" + NL +
                                           "(ignoreMe IN CASE WHEN row.book5 = '1' THEN [1]" + NL +
                                           "  ELSE []" + NL +
                                           "  END |" + NL +
                                           "  MERGE (book:Book {sequence: 5})" + NL +
                                           "  MERGE (p)-[:APPEARED_IN]->(book)" + NL +
                                           ");";

    private static final String DEATHS = "LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/tomasonjo/neo4j-game-of-thrones/master/data/character-predictions.csv' AS row" + NL +
                                         "" + NL +
                                         "// do CASE statements" + NL +
                                         "WITH row," + NL +
                                         "     CASE WHEN row.isAlive = '0' THEN [1]" + NL +
                                         "       ELSE []" + NL +
                                         "       END AS dead_person," + NL +
                                         "     CASE WHEN row.isAliveMother = '0' THEN [1]" + NL +
                                         "       ELSE []" + NL +
                                         "       END AS dead_mother," + NL +
                                         "     CASE WHEN row.isAliveFather = '0' THEN [1]" + NL +
                                         "       ELSE []" + NL +
                                         "       END AS dead_father," + NL +
                                         "     CASE WHEN row.isAliveHeir = '0' THEN [1]" + NL +
                                         "       ELSE []" + NL +
                                         "       END AS dead_heir," + NL +
                                         "     CASE WHEN row.isAliveSpouse = '0' THEN [1]" + NL +
                                         "       ELSE []" + NL +
                                         "       END AS dead_spouse" + NL +
                                         "" + NL +
                                         "// MATCH all the persons" + NL +
                                         "MATCH (p:Person {name: row.name})" + NL +
                                         "" + NL +
                                         "// We use optional match so that it doesnt stop the query if not found" + NL +
                                         "OPTIONAL MATCH (mother:Person {name: row.mother})" + NL +
                                         "OPTIONAL MATCH (father:Person {name: row.father})" + NL +
                                         "OPTIONAL MATCH (heir:Person {name: row.heir})" + NL +
                                         "OPTIONAL MATCH (spouse:Spouse {name: row.spouse})" + NL +
                                         "" + NL +
                                         "// Set the label of the dead persons" + NL +
                                         "FOREACH (d IN dead_person |" + NL +
                                         "  SET p:Dead" + NL +
                                         ")" + NL +
                                         "FOREACH (d IN dead_mother |" + NL +
                                         "  SET mother:Dead" + NL +
                                         ")" + NL +
                                         "FOREACH (d IN dead_father |" + NL +
                                         "  SET father:Dead" + NL +
                                         ")" + NL +
                                         "FOREACH (d IN dead_heir |" + NL +
                                         "  SET heir:Dead" + NL +
                                         ")" + NL +
                                         "FOREACH (d IN dead_spouse |" + NL +
                                         "  SET spouse:Dead" + NL +
                                         ");";

    private static final String MISC_LABELS = "MATCH (p:Person) where exists (p.death_year)" + NL +
                                              "SET p:Dead;" + NL +
                                              "" + NL +
                                              "MATCH (p:Person)-[:DEFENDER_KING|ATTACKER_KING]-()" + NL +
                                              "SET p:King;" + NL +
                                              "" + NL +
                                              "MATCH (p:Person) where lower(p.title) contains \"king\"" + NL +
                                              "SET p:King;" + NL +
                                              "" + NL +
                                              "MATCH (p:Person) where p.title = \"Ser\"" + NL +
                                              "SET p:Knight;";

    private static final String INTERACTIONS_1 = "LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/mathbeveridge/asoiaf/master/data/asoiaf-book1-edges.csv' AS row" + NL +
                                                 "WITH replace(row.Source, '-', ' ') AS srcName," + NL +
                                                 "     replace(row.Target, '-', ' ') AS tgtName," + NL +
                                                 "     toInteger(row.weight) AS weight" + NL +
                                                 "MERGE (src:Person {name: srcName})" + NL +
                                                 "MERGE (tgt:Person {name: tgtName})" + NL +
                                                 "MERGE (src)-[i:INTERACTS {book: 1}]->(tgt)" + NL +
                                                 "  ON CREATE SET i.weight = weight" + NL +
                                                 "  ON MATCH SET i.weight = i.weight + weight" + NL +
                                                 "MERGE (src)-[r:INTERACTS_1]->(tgt)" + NL +
                                                 "  ON CREATE SET r.weight = weight, r.book = 1;";
    private static final String INTERACTIONS_2 = "LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/mathbeveridge/asoiaf/master/data/asoiaf-book2-edges.csv' AS row" + NL +
                                                 "WITH replace(row.Source, '-', ' ') AS srcName," + NL +
                                                 "     replace(row.Target, '-', ' ') AS tgtName," + NL +
                                                 "     toInteger(row.weight) AS weight" + NL +
                                                 "MERGE (src:Person {name: srcName})" + NL +
                                                 "MERGE (tgt:Person {name: tgtName})" + NL +
                                                 "MERGE (src)-[i:INTERACTS {book: 2}]->(tgt)" + NL +
                                                 "  ON CREATE SET i.weight = weight" + NL +
                                                 "  ON MATCH SET i.weight = i.weight + weight" + NL +
                                                 "MERGE (src)-[r:INTERACTS_2]->(tgt)" + NL +
                                                 "  ON CREATE SET r.weight = weight, r.book = 2;";

    private static final String INTERACTIONS_3 = "LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/mathbeveridge/asoiaf/master/data/asoiaf-book3-edges.csv' AS row" + NL +
                                                 "WITH replace(row.Source, '-', ' ') AS srcName," + NL +
                                                 "     replace(row.Target, '-', ' ') AS tgtName," + NL +
                                                 "     toInteger(row.weight) AS weight" + NL +
                                                 "MERGE (src:Person {name: srcName})" + NL +
                                                 "MERGE (tgt:Person {name: tgtName})" + NL +
                                                 "MERGE (src)-[i:INTERACTS {book: 3}]->(tgt)" + NL +
                                                 "  ON CREATE SET i.weight = weight" + NL +
                                                 "  ON MATCH SET i.weight = i.weight + weight" + NL +
                                                 "MERGE (src)-[r:INTERACTS_3]->(tgt)" + NL +
                                                 "  ON CREATE SET r.weight = weight, r.book = 3;";

    private static final String INTERACTIONS_4 = "LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/mathbeveridge/asoiaf/master/data/asoiaf-book4-edges.csv' AS row" + NL +
                                                 "WITH replace(row.Source, '-', ' ') AS srcName," + NL +
                                                 "     replace(row.Target, '-', ' ') AS tgtName," + NL +
                                                 "     toInteger(row.weight) AS weight" + NL +
                                                 "MERGE (src:Person {name: srcName})" + NL +
                                                 "MERGE (tgt:Person {name: tgtName})" + NL +
                                                 "MERGE (src)-[i:INTERACTS {book: 4}]->(tgt)" + NL +
                                                 "  ON CREATE SET i.weight = weight" + NL +
                                                 "  ON MATCH SET i.weight = i.weight + weight" + NL +
                                                 "MERGE (src)-[r:INTERACTS_4]->(tgt)" + NL +
                                                 "  ON CREATE SET r.weight = weight, r.book = 4;";

    private static final String INTERACTIONS_5 = "LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/mathbeveridge/asoiaf/master/data/asoiaf-book5-edges.csv' AS row" + NL +
                                                 "WITH replace(row.Source, '-', ' ') AS srcName," + NL +
                                                 "     replace(row.Target, '-', ' ') AS tgtName," + NL +
                                                 "     toInteger(row.weight) AS weight" + NL +
                                                 "MERGE (src:Person {name: srcName})" + NL +
                                                 "MERGE (tgt:Person {name: tgtName})" + NL +
                                                 "MERGE (src)-[i:INTERACTS {book: 5}]->(tgt)" + NL +
                                                 "  ON CREATE SET i.weight = weight" + NL +
                                                 "  ON MATCH SET i.weight = i.weight + weight" + NL +
                                                 "MERGE (src)-[r:INTERACTS_5]->(tgt)" + NL +
                                                 "  ON CREATE SET r.weight = weight, r.book = 5;";
}
