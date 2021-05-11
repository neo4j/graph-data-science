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
package org.neo4j.graphalgo.embeddings.graphsage;

public final class GraphSageTestGraph {

    public static final String GDL =
        " CREATE" +
        "  (n0:Restaurant {dummyProp: 5.0, numEmployees: 2.0,   rating: 5.0, embedding: [1.0, 42.42] })" +
        ", (n1:Restaurant {dummyProp: 5.0, numEmployees: 2.0,   rating: 5.0, embedding: [1.0, 42.42] })" +
        ", (n2:Restaurant {dummyProp: 5.0, numEmployees: 2.0,   rating: 5.0, embedding: [1.0, 42.42] })" +
        ", (n3:Restaurant {dummyProp: 5.0, numEmployees: 2.0,   rating: 5.0, embedding: [1.0, 42.42] })" +
        ", (n4:Dish       {dummyProp: 5.0, numIngredients: 5.0, rating: 5.0})" +
        ", (n5:Dish       {dummyProp: 5.0, numIngredients: 5.0, rating: 5.0})" +
        ", (n6:Dish       {dummyProp: 5.0, numIngredients: 5.0, rating: 5.0})" +
        ", (n7:Dish       {dummyProp: 5.0, numIngredients: 5.0, rating: 5.0})" +
        ", (n8:Dish       {dummyProp: 5.0, numIngredients: 5.0, rating: 5.0})" +
        ", (n9:Dish       {dummyProp: 5.0, numIngredients: 5.0, rating: 5.0})" +
        ", (n10:Dish      {dummyProp: 5.0, numIngredients: 5.0, rating: 5.0})" +
        ", (n11:Dish      {dummyProp: 5.0, numIngredients: 5.0, rating: 5.0})" +
        ", (n12:Dish      {dummyProp: 5.0, numIngredients: 5.0, rating: 5.0})" +
        ", (n13:Customer  {dummyProp: 5.0, numPurchases: [5.0]})" +
        ", (n14:Customer  {dummyProp: 5.0, numPurchases: [5.0]})" +
        ", (n15:Customer  {dummyProp: 5.0, numPurchases: [5.0]})" +
        ", (n16:Customer  {dummyProp: 5.0, numPurchases: [5.0]})" +
        ", (n17:Customer  {dummyProp: 5.0, numPurchases: [5.0]})" +
        ", (n18:Customer  {dummyProp: 5.0, numPurchases: [5.0]})" +
        ", (n19:Customer  {dummyProp: 5.0, numPurchases: [5.0]})" +

        ", (n0)-[:SERVES { times: 5 }]->(n4)" +
        ", (n0)-[:SERVES { times: 5 }]->(n5)" +
        ", (n0)-[:SERVES { times: 5 }]->(n6)" +
        ", (n0)-[:SERVES { times: 5 }]->(n7)" +
        ", (n0)-[:SERVES { times: 5 }]->(n8)" +

        ", (n1)-[:SERVES { times: 5 }]->(n5)" +
        ", (n1)-[:SERVES { times: 5 }]->(n6)" +
        ", (n1)-[:SERVES { times: 5 }]->(n8)" +
        ", (n1)-[:SERVES { times: 5 }]->(n9)" +
        ", (n1)-[:SERVES { times: 5 }]->(n10)" +
        ", (n1)-[:SERVES { times: 5 }]->(n11)" +
        ", (n1)-[:SERVES { times: 5 }]->(n12)" +

        ", (n2)-[:SERVES { times: 5 }]->(n6)" +
        ", (n2)-[:SERVES { times: 5 }]->(n8)" +
        ", (n2)-[:SERVES { times: 5 }]->(n11)" +
        ", (n2)-[:SERVES { times: 5 }]->(n12)" +

        ", (n3)-[:SERVES { times: 5 }]->(n4)" +
        ", (n3)-[:SERVES { times: 5 }]->(n5)" +
        ", (n3)-[:SERVES { times: 5 }]->(n8)" +
        ", (n3)-[:SERVES { times: 5 }]->(n10)" +

        ", (n13)-[:ORDERED { times: 5 }]->(n4)" +
        ", (n13)-[:ORDERED { times: 5 }]->(n5)" +

        ", (n14)-[:ORDERED { times: 5 }]->(n4)" +
        ", (n14)-[:ORDERED { times: 5 }]->(n10)" +
        ", (n14)-[:ORDERED { times: 5 }]->(n12)" +

        ", (n15)-[:ORDERED { times: 5 }]->(n9)" +
        ", (n15)-[:ORDERED { times: 5 }]->(n12)" +

        ", (n16)-[:ORDERED { times: 5 }]->(n7)" +
        ", (n16)-[:ORDERED { times: 5 }]->(n8)" +
        ", (n16)-[:ORDERED { times: 5 }]->(n10)" +
        ", (n16)-[:ORDERED { times: 5 }]->(n12)" +

        ", (n17)-[:ORDERED { times: 5 }]->(n4)" +
        ", (n17)-[:ORDERED { times: 5 }]->(n5)" +
        ", (n17)-[:ORDERED { times: 5 }]->(n9)" +

        ", (n18)-[:ORDERED { times: 5 }]->(n5)" +
        ", (n18)-[:ORDERED { times: 5 }]->(n6)" +
        ", (n18)-[:ORDERED { times: 5 }]->(n11)" +

        ", (n19)-[:ORDERED { times: 5 }]->(n7)" +
        ", (n19)-[:ORDERED { times: 5 }]->(n11)";

    public static final String DUMMY_PROPERTY = "dummyProp";

    private GraphSageTestGraph() {}
}
