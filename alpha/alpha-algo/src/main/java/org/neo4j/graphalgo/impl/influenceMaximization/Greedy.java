package org.neo4j.graphalgo.impl.influenceMaximization;

import com.carrotsearch.hppc.LongDoubleScatterMap;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.concurrency.ParallelUtil;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.results.InfluenceMaximizationResult;

public class Greedy extends Algorithm<Greedy,Greedy>
{
    private final Graph graph;
    private final long k;
    private final double p;
    private final int mc;

    private final ExecutorService executorService;
    private final int concurrency;
    private final AllocationTracker tracker;
    private final ArrayList<Runnable> tasks;

    private final LongDoubleScatterMap kNodes;
    private final PriorityBlockingQueue<Pair<Long,Double>> spreads;

    private Pair<Long,Double> highest;

    /*
     * graph:   Graph
     * k:       Number of nodes
     * mc:      Number of Monte-Carlo simulations
     * p:       Propagation Probability
     */
    public Greedy( Graph graph, int k, double p, int mc, ExecutorService executorService, int concurrency, AllocationTracker tracker )
    {
        this.graph = graph;
        long nodeCount = graph.nodeCount();

        this.k = (k >= nodeCount) ? k : 3; // k >= nodeCount
        this.p = (p > 0 && p <= 1) ? p : 0.1; // 0 < p <= 1
        this.mc = (mc >= 1) ? mc : 1000; // mc >= 1

        this.executorService = executorService;
        this.concurrency = concurrency;
        this.tracker = tracker;
        this.tasks = new ArrayList<>();

        kNodes = new LongDoubleScatterMap( k );
        spreads = new PriorityBlockingQueue<>( (int) nodeCount, ( o1, o2 ) ->
        {
            int r = o2.getRight().compareTo( o1.getRight() );
            if ( r != 0 )
            {
                return r;
            }
            else
            {
                return o1.getLeft().compareTo( o2.getLeft() );
            }
        } );
    }

    @Override
    public Greedy compute()
    {
        //Find k nodes with largest marginal gain
        for ( long i = 0; i < k; i++ )
        {
            tasks.clear();
            spreads.clear();
            //Over nodes that are not yet in seed set find biggest marginal gain
            graph.forEachNode(
                    node ->
                    {
                        if ( !kNodes.containsKey( node ) )
                        {
                            tasks.add( new independentCascadeTask( graph, p, mc, node, kNodes.keys().toArray(), spreads, tracker ) );
                        }
                        if ( tasks.size() == concurrency )
                        {
                            ParallelUtil.run( tasks, executorService );
                            tasks.clear();
                        }
                        return true;
                    } );
            ParallelUtil.run( tasks, executorService );
            //Add the node with the highest spread to the seed set
            highest = spreads.poll();
            kNodes.put( highest.getLeft(), highest.getRight() );
        }
        return this;
    }

    @Override
    public Greedy me()
    {
        return this;
    }

    @Override
    public void release()
    {
    }

    public double getNodeSpread( long node )
    {
        return kNodes.getOrDefault( node, 0 );
    }

    public Stream<InfluenceMaximizationResult> resultStream()
    {
        return LongStream.of( kNodes.keys().toArray() )
                         .mapToObj( node -> new InfluenceMaximizationResult( graph.toOriginalNodeId( node ), getNodeSpread( node ) ) );
    }
}
