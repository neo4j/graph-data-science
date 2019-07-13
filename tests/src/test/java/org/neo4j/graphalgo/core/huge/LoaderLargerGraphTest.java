package org.neo4j.graphalgo.core.huge;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.HugeWeightMapping;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.huge.loader.HugeGraphFactory;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import static org.junit.Assert.assertEquals;

public final class LoaderLargerGraphTest {

    @Rule
    public ImpermanentDatabaseRule db = new ImpermanentDatabaseRule();

    @Test
    public void testBasicLoading() {
        // set low page shift so that 100k nodes will trigger the usage of the paged
        // huge array, which will trigger multi page code paths.
        // we import nodes in batches of 54600 nodes, using a page shift of 14
        // results in pages of 16384 elements, so we would have to write in multiple
        // pages for a single batch
        System.setProperty("org.neo4j.graphalgo.core.utils.paged.HugeArrays.singlePageShift", "14");
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
