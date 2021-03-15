package org.neo4j.graphalgo.impl.influenceMaximization;

import com.carrotsearch.hppc.LongScatterSet;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Random;
import java.util.concurrent.PriorityBlockingQueue;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArrayStack;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;

public final class independentCascadeTask implements Runnable
{
    private final Graph graph;
    private final double p;
    private final long mc;
    private final long candidateNode;
    private final long[] kNodes;
    private final PriorityBlockingQueue<Pair<Long,Double>> spreads;
    private final HugeObjectArray<LongScatterSet> active;
    private final HugeLongArrayStack newActive;
    private final Random rand;
    private double spread;

    public independentCascadeTask( Graph graph, double p, long mc, long candidateNode, long[] kNodes, PriorityBlockingQueue<Pair<Long,Double>> spreads,
                                   AllocationTracker tracker )
    {
        this.graph = graph.concurrentCopy();
        long nodeCount = graph.nodeCount();

        this.p = p;
        this.mc = mc;
        this.candidateNode = candidateNode;
        this.kNodes = kNodes;
        this.spreads = spreads;

        this.active = HugeObjectArray.newArray( LongScatterSet.class, 1, tracker );
        this.active.set( 0, new LongScatterSet() );
        this.newActive = HugeLongArrayStack.newStack( nodeCount, tracker );

        this.rand = new Random();
    }

    @Override
    public void run()
    {
        //Loop over the Monte-Carlo simulations
        spread = 0;
        for ( long i = 0; i < mc; i++ )
        {
            initStructures();
            spread += newActive.size();
            //For each newly active node, find its neighbors that become activated
            while ( !newActive.isEmpty() )
            {
                //Determine neighbors that become infected
                rand.setSeed( i );
                long node = newActive.pop();
                graph.forEachRelationship( node, ( source, target ) ->
                {
                    if ( rand.nextDouble() < p )
                    {
                        spread++;
                        if ( !active.get( 0 ).contains( target ) )
                        {
                            //Add newly activated nodes to the set of activated nodes
                            newActive.push( target );
                            active.get( 0 ).add( target );
                        }
                    }
                    return true;
                } );
            }
        }
        spreads.add( Pair.of( candidateNode, spread /= mc ) );
    }

    private void initStructures()
    {
        active.get( 0 ).clear();
        active.get( 0 ).add( candidateNode );
        active.get( 0 ).addAll( kNodes );

        newActive.push( candidateNode );
        for ( long kNode : kNodes )
        {
            newActive.push( kNode );
        }
    }
}
