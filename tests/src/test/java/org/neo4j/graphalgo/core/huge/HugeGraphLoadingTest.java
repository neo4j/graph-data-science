/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
package org.neo4j.graphalgo.core.huge;

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.HugeWeightMapping;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.huge.loader.HugeGraphFactory;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public final class HugeGraphLoadingTest {

    @Rule
    public ImpermanentDatabaseRule db = new ImpermanentDatabaseRule();

    @Parameterized.Parameters(name = "singlePageShift = {0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(
                // set low page shift so that 100k nodes will trigger the usage of the paged
                // huge array, which will trigger multi page code paths.
                // we import nodes in batches of 54600 nodes, using a page shift of 14
                // results in pages of 16384 elements, so we would have to write in multiple
                // pages for a single batch
                new Object[]{"14"},
                // default value
                new Object[]{"28"}
        );
    }

    private final String singlePageShift;

    public HugeGraphLoadingTest(String singlePageShift) {
        this.singlePageShift = singlePageShift;
    }

    @Test
    public void testLoading() {
        System.setProperty("org.neo4j.graphalgo.core.utils.paged.HugeArrays.singlePageShift", singlePageShift);
        // something larger than one batch
        int nodeCount = 60_000;
        Label label = Label.label("Foo");
        db.executeAndCommit(gdb -> {
            for (int j = 0; j < nodeCount; j++) {
                Node node = gdb.createNode(label);
                node.setProperty("bar", node.getId());
            }
        });

        final Graph graph = new GraphLoader(db)
                .withDirection(Direction.OUTGOING)
                .withLabel(label)
                .withOptionalNodeProperties(PropertyMapping.of("bar", "bar", -1.0))
                .load(HugeGraphFactory.class);

        HugeWeightMapping nodeProperties = graph.nodeProperties("bar");
        long propertyCountDiff = nodeCount - nodeProperties.size();
        String errorMessage = String.format(
                "lost %d properties during import",
                propertyCountDiff
        );
        assertEquals(errorMessage, 0, propertyCountDiff);

        for (int i = 0; i < nodeCount; i++) {
            double weight = nodeProperties.nodeWeight(i);
            long expected = graph.toOriginalNodeId(i);
            String message = String.format("Property for node %d (neo = %d) was overwritten.", i, expected);
            assertEquals(message, expected, (long) weight);
        }
    }
}
