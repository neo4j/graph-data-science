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

package org.neo4j.graphalgo;

import org.neo4j.graphalgo.compat.MapUtil;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.neo4j.graphalgo.QueryRunner.runQuery;

public class GOTQuery {

    public static void importGot(GraphDatabaseAPI db) {
        GOT_IMPORT_QUERIES.forEach(query -> runQuery(db, query, GOT_NAMES_MAP));
    }

    private static Map<String, Object> GOT_NAMES_MAP = MapUtil.map("map", MapUtil.map(
        "Aemon Targaryen (Maester Aemon)", "Aemon Targaryen (son of Maekar I)",
        "Arstan",                          "Barristan Selmy",
        "Garin (orphan)",                  "Garin (Orphan)",
        "Hareth (Moles Town)",             "Hareth (Mole's Town)",
        "Jaqen Hghar",                     "Jaqen H'ghar",
        "Lommy Greenhands",                "Lommy",
        "Rattleshirt",                     "Lord of Bones",
        "Thoros of Myr",                   "Thoros"));

    private static String GOT_IMPORT_QUERY = "LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/tomasonjo/neo4j-game-of-thrones/master/data/battles.csv' AS row\n" +
                                                  "MERGE (b:Battle {name: row.name})\n" +
                                                  "  ON CREATE SET b.year = toInteger(row.year),\n" +
                                                  "  b.summer = row.summer,\n" +
                                                  "  b.major_death = row.major_death,\n" +
                                                  "  b.major_capture = row.major_capture,\n" +
                                                  "  b.note = row.note,\n" +
                                                  "  b.battle_type = row.battle_type,\n" +
                                                  "  b.attacker_size = toInteger(row.attacker_size),\n" +
                                                  "  b.defender_size = toInteger(row.defender_size);\n" +
                                                  "\n" +
                                                  "LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/tomasonjo/neo4j-game-of-thrones/master/data/battles.csv' AS row\n" +
                                                  "\n" +
                                                  "// there is only attacker_outcome in the data,\n" +
                                                  "// so we do a CASE statement for defender_outcome\n" +
                                                  "WITH row,\n" +
                                                  "     CASE WHEN row.attacker_outcome = 'win' THEN 'loss'\n" +
                                                  "       ELSE 'win'\n" +
                                                  "       END AS defender_outcome\n" +
                                                  "\n" +
                                                  "// match the battle\n" +
                                                  "MATCH (b:Battle {name: row.name})\n" +
                                                  "\n" +
                                                  "// all battles have atleast one attacker so we don't have to use foreach trick\n" +
                                                  "MERGE (attacker1:House {name: row.attacker_1})\n" +
                                                  "MERGE (attacker1)-[a1:ATTACKER]->(b)\n" +
                                                  "  ON CREATE SET a1.outcome = row.attacker_outcome\n" +
                                                  "\n" +
                                                  "// When we want to skip null values we can use foreach trick\n" +
                                                  "FOREACH\n" +
                                                  "(ignoreMe IN CASE WHEN row.defender_1 IS NOT NULL THEN [1]\n" +
                                                  "  ELSE []\n" +
                                                  "  END |\n" +
                                                  "  MERGE (defender1:House {name: row.defender_1})\n" +
                                                  "  MERGE (defender1)-[d1:DEFENDER]->(b)\n" +
                                                  "    ON CREATE SET d1.outcome = defender_outcome\n" +
                                                  ")\n" +
                                                  "FOREACH\n" +
                                                  "(ignoreMe IN CASE WHEN row.defender_2 IS NOT NULL THEN [1]\n" +
                                                  "  ELSE []\n" +
                                                  "  END |\n" +
                                                  "  MERGE (defender2:House {name: row.defender_2})\n" +
                                                  "  MERGE (defender2)-[d2:DEFENDER]->(b)\n" +
                                                  "    ON CREATE SET d2.outcome = defender_outcome\n" +
                                                  ")\n" +
                                                  "FOREACH\n" +
                                                  "(ignoreMe IN CASE WHEN row.attacker_2 IS NOT NULL THEN [1]\n" +
                                                  "  ELSE []\n" +
                                                  "  END |\n" +
                                                  "  MERGE (attacker2:House {name: row.attacker_2})\n" +
                                                  "  MERGE (attacker2)-[a2:ATTACKER]->(b)\n" +
                                                  "    ON CREATE SET a2.outcome = row.attacker_outcome\n" +
                                                  ")\n" +
                                                  "FOREACH\n" +
                                                  "(ignoreMe IN CASE WHEN row.attacker_3 IS NOT NULL THEN [1]\n" +
                                                  "  ELSE []\n" +
                                                  "  END |\n" +
                                                  "  MERGE (attacker2:House {name: row.attacker_3})\n" +
                                                  "  MERGE (attacker3)-[a3:ATTACKER]->(b)\n" +
                                                  "    ON CREATE SET a3.outcome = row.attacker_outcome\n" +
                                                  ")\n" +
                                                  "FOREACH\n" +
                                                  "(ignoreMe IN CASE WHEN row.attacker_4 IS NOT NULL THEN [1]\n" +
                                                  "  ELSE []\n" +
                                                  "  END |\n" +
                                                  "  MERGE (attacker4:House {name: row.attacker_4})\n" +
                                                  "  MERGE (attacker4)-[a4:ATTACKER]->(b)\n" +
                                                  "    ON CREATE SET a4.outcome = row.attacker_outcome\n" +
                                                  ");\n" +
                                                  "\n" +
                                                  "LOAD CSV WITH HEADERS FROM\n" +
                                                  "'https://raw.githubusercontent.com/tomasonjo/neo4j-game-of-thrones/master/data/battles.csv'\n" +
                                                  "AS row\n" +
                                                  "MATCH (b:Battle {name: row.name})\n" +
                                                  "\n" +
                                                  "// We use coalesce, so that null values are replaced with \"Unknown\"\n" +
                                                  "MERGE (location:Location {name: coalesce(row.location, 'Unknown')})\n" +
                                                  "MERGE (b)-[:IS_IN]->(location)\n" +
                                                  "MERGE (region:Region {name: row.region})\n" +
                                                  "MERGE (location)-[:IS_IN]->(region);\n" +
                                                  "\n" +
                                                  "LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/tomasonjo/neo4j-game-of-thrones/master/data/battles.csv' AS row\n" +
                                                  "\n" +
                                                  "// We split the columns that may contain more than one person\n" +
                                                  "WITH row,\n" +
                                                  "     split(row.attacker_commander, ',') AS att_commanders,\n" +
                                                  "     split(row.defender_commander, ',') AS def_commanders,\n" +
                                                  "     split(row.attacker_king, '/') AS att_kings,\n" +
                                                  "     split(row.defender_king, '/') AS def_kings,\n" +
                                                  "     row.attacker_outcome AS att_outcome,\n" +
                                                  "     CASE WHEN row.attacker_outcome = 'win' THEN 'loss'\n" +
                                                  "       ELSE 'win'\n" +
                                                  "       END AS def_outcome\n" +
                                                  "MATCH (b:Battle {name: row.name})\n" +
                                                  "\n" +
                                                  "// we unwind a list\n" +
                                                  "UNWIND att_commanders AS att_commander\n" +
                                                  "MERGE (p:Person {name: trim(att_commander)})\n" +
                                                  "MERGE (p)-[ac:ATTACKER_COMMANDER]->(b)\n" +
                                                  "  ON CREATE SET ac.outcome = att_outcome\n" +
                                                  "\n" +
                                                  "// to end the unwind and correct cardinality(number of rows)\n" +
                                                  "// we use any aggregation function ( e.g. count(*))\n" +
                                                  "WITH b, def_commanders, def_kings, att_kings, att_outcome, def_outcome,\n" +
                                                  "     COUNT(*) AS c1\n" +
                                                  "UNWIND def_commanders AS def_commander\n" +
                                                  "MERGE (p:Person {name: trim(def_commander)})\n" +
                                                  "MERGE (p)-[dc:DEFENDER_COMMANDER]->(b)\n" +
                                                  "  ON CREATE SET dc.outcome = def_outcome\n" +
                                                  "\n" +
                                                  "// reset cardinality with an aggregation function (end the unwind)\n" +
                                                  "WITH b, def_kings, att_kings, att_outcome, def_outcome, COUNT(*) AS c2\n" +
                                                  "UNWIND def_kings AS def_king\n" +
                                                  "MERGE (p:Person {name: trim(def_king)})\n" +
                                                  "MERGE (p)-[dk:DEFENDER_KING]->(b)\n" +
                                                  "  ON CREATE SET dk.outcome = def_outcome\n" +
                                                  "\n" +
                                                  "// reset cardinality with an aggregation function (end the unwind)\n" +
                                                  "WITH b, att_kings, att_outcome, COUNT(*) AS c3\n" +
                                                  "UNWIND att_kings AS att_king\n" +
                                                  "MERGE (p:Person {name: trim(att_king)})\n" +
                                                  "MERGE (p)-[ak:ATTACKER_KING]->(b)\n" +
                                                  "  ON CREATE SET ak.outcome = att_outcome;\n" +
                                                  "\n" +
                                                  "LOAD CSV WITH HEADERS FROM\n" +
                                                  "'https://raw.githubusercontent.com/tomasonjo/neo4j-game-of-thrones/master/data/character-deaths.csv'\n" +
                                                  "AS row\n" +
                                                  "\n" +
                                                  "// we can use CASE in a WITH statement\n" +
                                                  "WITH row,\n" +
                                                  "     CASE WHEN row.Nobility = '1' THEN 'Noble'\n" +
                                                  "       ELSE 'Commoner'\n" +
                                                  "       END AS status_value\n" +
                                                  "\n" +
                                                  "// as seen above we remove \"House \" for better linking\n" +
                                                  "MERGE (house:House {name: replace(row.Allegiances, 'House ', '')})\n" +
                                                  "MERGE (person:Person {name: row.Name})\n" +
                                                  "\n" +
                                                  "// we can also use CASE statement inline\n" +
                                                  "SET person.gender = CASE WHEN row.Gender = '1' THEN 'male'\n" +
                                                  "  ELSE 'female'\n" +
                                                  "  END,\n" +
                                                  "person.book_intro_chapter = row.`Book Intro Chapter`,\n" +
                                                  "person.book_death_chapter = row.`Death Chapter`,\n" +
                                                  "person.book_of_death = row.`Book of Death`,\n" +
                                                  "person.death_year = toINT(row.`Death Year`)\n" +
                                                  "MERGE (person)-[:BELONGS_TO]->(house)\n" +
                                                  "MERGE (status:Status {name: status_value})\n" +
                                                  "MERGE (person)-[:HAS_STATUS]->(status)\n" +
                                                  "\n" +
                                                  "// doing the foreach trick to skip null values\n" +
                                                  "FOREACH\n" +
                                                  "(ignoreMe IN CASE WHEN row.GoT = '1' THEN [1]\n" +
                                                  "  ELSE []\n" +
                                                  "  END |\n" +
                                                  "  MERGE (book1:Book {sequence: 1})\n" +
                                                  "    ON CREATE SET book1.name = 'Game of thrones'\n" +
                                                  "  MERGE (person)-[:APPEARED_IN]->(book1)\n" +
                                                  ")\n" +
                                                  "FOREACH\n" +
                                                  "(ignoreMe IN CASE WHEN row.CoK = '1' THEN [1]\n" +
                                                  "  ELSE []\n" +
                                                  "  END |\n" +
                                                  "  MERGE (book2:Book {sequence: 2})\n" +
                                                  "    ON CREATE SET book2.name = 'Clash of kings'\n" +
                                                  "  MERGE (person)-[:APPEARED_IN]->(book2)\n" +
                                                  ")\n" +
                                                  "FOREACH\n" +
                                                  "(ignoreMe IN CASE WHEN row.SoS = '1' THEN [1]\n" +
                                                  "  ELSE []\n" +
                                                  "  END |\n" +
                                                  "  MERGE (book3:Book {sequence: 3})\n" +
                                                  "    ON CREATE SET book3.name = 'Storm of swords'\n" +
                                                  "  MERGE (person)-[:APPEARED_IN]->(book3)\n" +
                                                  ")\n" +
                                                  "FOREACH\n" +
                                                  "(ignoreMe IN CASE WHEN row.FfC = '1' THEN [1]\n" +
                                                  "  ELSE []\n" +
                                                  "  END |\n" +
                                                  "  MERGE (book4:Book {sequence: 4})\n" +
                                                  "    ON CREATE SET book4.name = 'Feast for crows'\n" +
                                                  "  MERGE (person)-[:APPEARED_IN]->(book4)\n" +
                                                  ")\n" +
                                                  "FOREACH\n" +
                                                  "(ignoreMe IN CASE WHEN row.DwD = '1' THEN [1]\n" +
                                                  "  ELSE []\n" +
                                                  "  END |\n" +
                                                  "  MERGE (book5:Book {sequence: 5})\n" +
                                                  "    ON CREATE SET book5.name = 'Dance with dragons'\n" +
                                                  "  MERGE (person)-[:APPEARED_IN]->(book5)\n" +
                                                  ")\n" +
                                                  "FOREACH\n" +
                                                  "(ignoreMe IN CASE WHEN row.`Book of Death` IS NOT NULL THEN [1]\n" +
                                                  "  ELSE []\n" +
                                                  "  END |\n" +
                                                  "  MERGE (book:Book {sequence: toInt(row.`Book of Death`)})\n" +
                                                  "  MERGE (person)-[:DIED_IN]->(book)\n" +
                                                  ");\n" +
                                                  "\n" +
                                                  "LOAD CSV WITH HEADERS FROM\n" +
                                                  "'https://raw.githubusercontent.com/tomasonjo/neo4j-game-of-thrones/master/data/character-predictions.csv'\n" +
                                                  "AS row\n" +
                                                  "MERGE (p:Person {name: row.name})\n" +
                                                  "// set properties on the person node\n" +
                                                  "SET p.title = row.title,\n" +
                                                  "p.death_year = toINT(row.DateoFdeath),\n" +
                                                  "p.birth_year = toINT(row.dateOfBirth),\n" +
                                                  "p.age = toINT(row.age),\n" +
                                                  "p.gender = CASE WHEN row.male = '1' THEN 'male'\n" +
                                                  "  ELSE 'female'\n" +
                                                  "  END\n" +
                                                  "\n" +
                                                  "// doing the foreach trick to skip null values\n" +
                                                  "FOREACH\n" +
                                                  "(ignoreMe IN CASE WHEN row.mother IS NOT NULL THEN [1]\n" +
                                                  "  ELSE []\n" +
                                                  "  END |\n" +
                                                  "  MERGE (mother:Person {name: row.mother})\n" +
                                                  "  MERGE (p)-[:RELATED_TO {name: 'mother'}]->(mother)\n" +
                                                  ")\n" +
                                                  "FOREACH\n" +
                                                  "(ignoreMe IN CASE WHEN row.spouse IS NOT NULL THEN [1]\n" +
                                                  "  ELSE []\n" +
                                                  "  END |\n" +
                                                  "  MERGE (spouse:Person {name: row.spouse})\n" +
                                                  "  MERGE (p)-[:RELATED_TO {name: 'spouse'}]->(spouse)\n" +
                                                  ")\n" +
                                                  "FOREACH\n" +
                                                  "(ignoreMe IN CASE WHEN row.father IS NOT NULL THEN [1]\n" +
                                                  "  ELSE []\n" +
                                                  "  END |\n" +
                                                  "  MERGE (father:Person {name: row.father})\n" +
                                                  "  MERGE (p)-[:RELATED_TO {name: 'father'}]->(father)\n" +
                                                  ")\n" +
                                                  "FOREACH\n" +
                                                  "(ignoreMe IN CASE WHEN row.heir IS NOT NULL THEN [1]\n" +
                                                  "  ELSE []\n" +
                                                  "  END |\n" +
                                                  "  MERGE (heir:Person {name: row.heir})\n" +
                                                  "  MERGE (p)-[:RELATED_TO {name: 'heir'}]->(heir)\n" +
                                                  ")\n" +
                                                  "\n" +
                                                  "// we remove \"House \" from the value for better linking of data - this didn't run in the original, but I think it should now - there was a bug in the replace\n" +
                                                  "FOREACH\n" +
                                                  "(ignoreMe IN CASE WHEN row.house IS NOT NULL THEN [1]\n" +
                                                  "  ELSE []\n" +
                                                  "  END |\n" +
                                                  "  MERGE (house:House {name: replace(row.house, 'House ', '')})\n" +
                                                  "  MERGE (p)-[:BELONGS_TO]->(house)\n" +
                                                  ");\n" +
                                                  "\n" +
                                                  "LOAD CSV WITH HEADERS FROM\n" +
                                                  "'https://raw.githubusercontent.com/tomasonjo/neo4j-game-of-thrones/master/data/character-predictions.csv'\n" +
                                                  "AS row\n" +
                                                  "\n" +
                                                  "// match person\n" +
                                                  "MERGE (p:Person {name: row.name})\n" +
                                                  "\n" +
                                                  "// doing the foreach trick... we lower row.culture for better linking\n" +
                                                  "FOREACH\n" +
                                                  "(ignoreMe IN CASE WHEN row.culture IS NOT NULL THEN [1]\n" +
                                                  "  ELSE []\n" +
                                                  "  END |\n" +
                                                  "  MERGE (culture:Culture {name: lower(row.culture)})\n" +
                                                  "  MERGE (p)-[:MEMBER_OF_CULTURE]->(culture)\n" +
                                                  ")\n" +
                                                  "FOREACH\n" +
                                                  "(ignoreMe IN CASE WHEN row.book1 = '1' THEN [1]\n" +
                                                  "  ELSE []\n" +
                                                  "  END |\n" +
                                                  "  MERGE (book:Book {sequence: 1})\n" +
                                                  "  MERGE (p)-[:APPEARED_IN]->(book)\n" +
                                                  ")\n" +
                                                  "FOREACH\n" +
                                                  "(ignoreMe IN CASE WHEN row.book2 = '1' THEN [1]\n" +
                                                  "  ELSE []\n" +
                                                  "  END |\n" +
                                                  "  MERGE (book:Book {sequence: 2})\n" +
                                                  "  MERGE (p)-[:APPEARED_IN]->(book)\n" +
                                                  ")\n" +
                                                  "FOREACH\n" +
                                                  "(ignoreMe IN CASE WHEN row.book3 = '1' THEN [1]\n" +
                                                  "  ELSE []\n" +
                                                  "  END |\n" +
                                                  "  MERGE (book:Book {sequence: 3})\n" +
                                                  "  MERGE (p)-[:APPEARED_IN]->(book)\n" +
                                                  ")\n" +
                                                  "FOREACH\n" +
                                                  "(ignoreMe IN CASE WHEN row.book4 = '1' THEN [1]\n" +
                                                  "  ELSE []\n" +
                                                  "  END |\n" +
                                                  "  MERGE (book:Book {sequence: 4})\n" +
                                                  "  MERGE (p)-[:APPEARED_IN]->(book)\n" +
                                                  ")\n" +
                                                  "FOREACH\n" +
                                                  "(ignoreMe IN CASE WHEN row.book5 = '1' THEN [1]\n" +
                                                  "  ELSE []\n" +
                                                  "  END |\n" +
                                                  "  MERGE (book:Book {sequence: 5})\n" +
                                                  "  MERGE (p)-[:APPEARED_IN]->(book)\n" +
                                                  ");\n" +
                                                  "\n" +
                                                  "LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/tomasonjo/neo4j-game-of-thrones/master/data/character-predictions.csv' AS row\n" +
                                                  "\n" +
                                                  "// do CASE statements\n" +
                                                  "WITH row,\n" +
                                                  "     CASE WHEN row.isAlive = '0' THEN [1]\n" +
                                                  "       ELSE []\n" +
                                                  "       END AS dead_person,\n" +
                                                  "     CASE WHEN row.isAliveMother = '0' THEN [1]\n" +
                                                  "       ELSE []\n" +
                                                  "       END AS dead_mother,\n" +
                                                  "     CASE WHEN row.isAliveFather = '0' THEN [1]\n" +
                                                  "       ELSE []\n" +
                                                  "       END AS dead_father,\n" +
                                                  "     CASE WHEN row.isAliveHeir = '0' THEN [1]\n" +
                                                  "       ELSE []\n" +
                                                  "       END AS dead_heir,\n" +
                                                  "     CASE WHEN row.isAliveSpouse = '0' THEN [1]\n" +
                                                  "       ELSE []\n" +
                                                  "       END AS dead_spouse\n" +
                                                  "\n" +
                                                  "// MATCH all the persons\n" +
                                                  "MATCH (p:Person {name: row.name})\n" +
                                                  "\n" +
                                                  "// We use optional match so that it doesnt stop the query if not found\n" +
                                                  "OPTIONAL MATCH (mother:Person {name: row.mother})\n" +
                                                  "OPTIONAL MATCH (father:Person {name: row.father})\n" +
                                                  "OPTIONAL MATCH (heir:Person {name: row.heir})\n" +
                                                  "OPTIONAL MATCH (spouse:Spouse {name: row.spouse})\n" +
                                                  "\n" +
                                                  "// Set the label of the dead persons\n" +
                                                  "FOREACH (d IN dead_person |\n" +
                                                  "  SET p:Dead\n" +
                                                  ")\n" +
                                                  "FOREACH (d IN dead_mother |\n" +
                                                  "  SET mother:Dead\n" +
                                                  ")\n" +
                                                  "FOREACH (d IN dead_father |\n" +
                                                  "  SET father:Dead\n" +
                                                  ")\n" +
                                                  "FOREACH (d IN dead_heir |\n" +
                                                  "  SET heir:Dead\n" +
                                                  ")\n" +
                                                  "FOREACH (d IN dead_spouse |\n" +
                                                  "  SET spouse:Dead\n" +
                                                  ");\n" +
                                                  "\n" +
                                                  "MATCH (p:Person) where exists (p.death_year)\n" +
                                                  "SET p:Dead;\n" +
                                                  "\n" +
                                                  "MATCH (p:Person)-[:DEFENDER_KING|ATTACKER_KING]-()\n" +
                                                  "SET p:King;\n" +
                                                  "\n" +
                                                  "MATCH (p:Person) where lower(p.title) contains \"king\"\n" +
                                                  "SET p:King;\n" +
                                                  "\n" +
                                                  "MATCH (p:Person) where p.title = \"Ser\"\n" +
                                                  "SET p:Knight;\n" +
                                                  "\n" +
                                                  "LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/mathbeveridge/asoiaf/master/data/asoiaf-book1-edges.csv' AS row\n" +
                                                  "WITH replace(row.Source, '-', ' ') AS srcName,\n" +
                                                  "     replace(row.Target, '-', ' ') AS tgtName,\n" +
                                                  "     toInteger(row.weight) AS weight\n" +
                                                  "MERGE (src:Person {name: coalesce($map[srcName], srcName)})\n" +
                                                  "MERGE (tgt:Person {name: coalesce($map[tgtName], tgtName)})\n" +
                                                  "MERGE (src)-[i:INTERACTS {book: 1}]->(tgt)\n" +
                                                  "  ON CREATE SET i.weight = weight\n" +
                                                  "  ON MATCH SET i.weight = i.weight + weight\n" +
                                                  "MERGE (src)-[r:INTERACTS_1]->(tgt)\n" +
                                                  "  ON CREATE SET r.weight = weight, r.book = 1;\n" +
                                                  "\n" +
                                                  "LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/mathbeveridge/asoiaf/master/data/asoiaf-book2-edges.csv' AS row\n" +
                                                  "WITH replace(row.Source, '-', ' ') AS srcName,\n" +
                                                  "     replace(row.Target, '-', ' ') AS tgtName,\n" +
                                                  "     toInteger(row.weight) AS weight\n" +
                                                  "MERGE (src:Person {name: coalesce($map[srcName], srcName)})\n" +
                                                  "MERGE (tgt:Person {name: coalesce($map[tgtName], tgtName)})\n" +
                                                  "MERGE (src)-[i:INTERACTS {book: 2}]->(tgt)\n" +
                                                  "  ON CREATE SET i.weight = weight\n" +
                                                  "  ON MATCH SET i.weight = i.weight + weight\n" +
                                                  "MERGE (src)-[r:INTERACTS_2]->(tgt)\n" +
                                                  "  ON CREATE SET r.weight = weight, r.book = 2;\n" +
                                                  "\n" +
                                                  "LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/mathbeveridge/asoiaf/master/data/asoiaf-book3-edges.csv' AS row\n" +
                                                  "WITH replace(row.Source, '-', ' ') AS srcName,\n" +
                                                  "     replace(row.Target, '-', ' ') AS tgtName,\n" +
                                                  "     toInteger(row.weight) AS weight\n" +
                                                  "MERGE (src:Person {name: coalesce($map[srcName], srcName)})\n" +
                                                  "MERGE (tgt:Person {name: coalesce($map[tgtName], tgtName)})\n" +
                                                  "MERGE (src)-[i:INTERACTS {book: 3}]->(tgt)\n" +
                                                  "  ON CREATE SET i.weight = weight\n" +
                                                  "  ON MATCH SET i.weight = i.weight + weight\n" +
                                                  "MERGE (src)-[r:INTERACTS_3]->(tgt)\n" +
                                                  "  ON CREATE SET r.weight = weight, r.book = 3;\n" +
                                                  "\n" +
                                                  "LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/mathbeveridge/asoiaf/master/data/asoiaf-book4-edges.csv' AS row\n" +
                                                  "WITH replace(row.Source, '-', ' ') AS srcName,\n" +
                                                  "     replace(row.Target, '-', ' ') AS tgtName,\n" +
                                                  "     toInteger(row.weight) AS weight\n" +
                                                  "MERGE (src:Person {name: coalesce($map[srcName], srcName)})\n" +
                                                  "MERGE (tgt:Person {name: coalesce($map[tgtName], tgtName)})\n" +
                                                  "MERGE (src)-[i:INTERACTS {book: 4}]->(tgt)\n" +
                                                  "  ON CREATE SET i.weight = weight\n" +
                                                  "  ON MATCH SET i.weight = i.weight + weight\n" +
                                                  "MERGE (src)-[r:INTERACTS_4]->(tgt)\n" +
                                                  "  ON CREATE SET r.weight = weight, r.book = 4;\n" +
                                                  "\n" +
                                                  "LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/mathbeveridge/asoiaf/master/data/asoiaf-book5-edges.csv' AS row\n" +
                                                  "WITH replace(row.Source, '-', ' ') AS srcName,\n" +
                                                  "     replace(row.Target, '-', ' ') AS tgtName,\n" +
                                                  "     toInteger(row.weight) AS weight\n" +
                                                  "MERGE (src:Person {name: coalesce($map[srcName], srcName)})\n" +
                                                  "MERGE (tgt:Person {name: coalesce($map[tgtName], tgtName)})\n" +
                                                  "MERGE (src)-[i:INTERACTS {book: 5}]->(tgt)\n" +
                                                  "  ON CREATE SET i.weight = weight\n" +
                                                  "  ON MATCH SET i.weight = i.weight + weight\n" +
                                                  "MERGE (src)-[r:INTERACTS_5]->(tgt)\n" +
                                                  "  ON CREATE SET r.weight = weight, r.book = 5;";

    private static List<String> GOT_IMPORT_QUERIES = Arrays.asList(GOT_IMPORT_QUERY.split(";"));
}
