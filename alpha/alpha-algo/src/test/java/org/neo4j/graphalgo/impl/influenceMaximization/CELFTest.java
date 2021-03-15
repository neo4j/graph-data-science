package org.neo4j.graphalgo.impl.influenceMaximization;

import org.junit.jupiter.api.Test;

import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.extension.GdlExtension;
import org.neo4j.graphalgo.extension.GdlGraph;
import org.neo4j.graphalgo.extension.IdFunction;
import org.neo4j.graphalgo.extension.Inject;

import static org.assertj.core.api.Assertions.assertThat;

@GdlExtension
final class CELFTest
{
    /**
     *      (c)-----|
     *     /(d)\----|-|
     *    //(e)\\---|-|-|
     *   ///(f)\\\--|-|-|-|
     *  ////   \\\\ | | | |
     *  (a)     (b) | | | |
     *  \\\\   //// | | | |
     *   \\\(g)///--| | | |
     *    \\(h)//-----| | |
     *     \(i)/--------| |
     *      (j)-----------|
     */
    @GdlGraph( orientation = Orientation.NATURAL )
    private static final String DB_CYPHER =
            "CREATE " +
            "  (a:Node)" +
            ", (b:Node)" +
            ", (c:Node)" +
            ", (d:Node)" +
            ", (e:Node)" +
            ", (f:Node)" +
            ", (g:Node)" +
            ", (h:Node)" +
            ", (i:Node)" +
            ", (j:Node)" +

            ", (a)-[:RELATIONSHIP]->(c)" +
            ", (a)-[:RELATIONSHIP]->(d)" +
            ", (a)-[:RELATIONSHIP]->(e)" +
            ", (a)-[:RELATIONSHIP]->(f)" +
            ", (a)-[:RELATIONSHIP]->(g)" +
            ", (a)-[:RELATIONSHIP]->(h)" +
            ", (a)-[:RELATIONSHIP]->(i)" +
            ", (a)-[:RELATIONSHIP]->(j)" +

            ", (b)-[:RELATIONSHIP]->(c)" +
            ", (b)-[:RELATIONSHIP]->(d)" +
            ", (b)-[:RELATIONSHIP]->(e)" +
            ", (b)-[:RELATIONSHIP]->(f)" +
            ", (b)-[:RELATIONSHIP]->(g)" +
            ", (b)-[:RELATIONSHIP]->(h)" +
            ", (b)-[:RELATIONSHIP]->(i)" +
            ", (b)-[:RELATIONSHIP]->(j)" +

            ", (c)-[:RELATIONSHIP]->(g)" +
            ", (d)-[:RELATIONSHIP]->(h)" +
            ", (e)-[:RELATIONSHIP]->(i)" +
            ", (f)-[:RELATIONSHIP]->(j)";

    @Inject
    private Graph graph;

    @Inject
    private IdFunction idFunction;

    @Test
    void testSpreadFirstGraph()
    {
        CELF celf = new CELF( graph, 10, 0.2, 10, Pools.DEFAULT, 2, AllocationTracker.empty() );
        celf.compute();

        assertThat( celf.getNodeSpread( idFunction.of( "a" ) ) ).isEqualTo( 2.2 );
        assertThat( celf.getNodeSpread( idFunction.of( "b" ) ) ).isEqualTo( 4.4 );
        assertThat( celf.getNodeSpread( idFunction.of( "c" ) ) ).isEqualTo( 5.4 );
        assertThat( celf.getNodeSpread( idFunction.of( "d" ) ) ).isEqualTo( 6.4 );
        assertThat( celf.getNodeSpread( idFunction.of( "e" ) ) ).isEqualTo( 7.4 );
        assertThat( celf.getNodeSpread( idFunction.of( "f" ) ) ).isEqualTo( 8.4 );
        assertThat( celf.getNodeSpread( idFunction.of( "g" ) ) ).isEqualTo( 9.4 );
        assertThat( celf.getNodeSpread( idFunction.of( "h" ) ) ).isEqualTo( 10.4 );
        assertThat( celf.getNodeSpread( idFunction.of( "i" ) ) ).isEqualTo( 11.4 );
        assertThat( celf.getNodeSpread( idFunction.of( "j" ) ) ).isEqualTo( 12.4 );
    }
}
