package org.neo4j.graphalgo.influenceΜaximization;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.AlgorithmFactory;
import org.neo4j.graphalgo.AlphaAlgorithmFactory;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.impl.influenceMaximization.Greedy;
import org.neo4j.graphalgo.result.AbstractResultBuilder;
import org.neo4j.graphalgo.results.InfluenceMaximizationResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import static org.neo4j.procedure.Mode.READ;

public class GreedyProc extends AlgoBaseProc<Greedy,Greedy,InfluenceMaximizationConfig>
{
    private static final String DESCRIPTION =
            "Greedy algorithm for Influence Maximization aims to find `k` nodes that maximize the expected spread of influence in the network";

    @Procedure( name = "gds.alpha.greedy.stream", mode = READ )
    @Description( DESCRIPTION )
    public Stream<InfluenceMaximizationResult> stream(
            @Name( value = "graphName" ) Object graphNameOrConfig,
            @Name( value = "configuration", defaultValue = "{}" ) Map<String,Object> configuration )
    {

        ComputationResult<Greedy,Greedy,InfluenceMaximizationConfig> computationResult = compute( graphNameOrConfig, configuration );

        if ( computationResult.graph().isEmpty() )
        {
            computationResult.graph().release();
            return Stream.empty();
        }

        computationResult.graph().release();
        return computationResult.algorithm().resultStream();
    }

    @Procedure( name = "gds.alpha.greedy.stats", mode = READ )
    @Description( DESCRIPTION )
    public Stream<InfluenceMaximizationResult.Stats> stats(
            @Name( value = "graphName" ) Object graphNameOrConfig,
            @Name( value = "configuration", defaultValue = "{}" ) Map<String,Object> configuration
    )
    {
        ComputationResult<Greedy,Greedy,InfluenceMaximizationConfig> computationResult = compute( graphNameOrConfig, configuration );

        InfluenceMaximizationConfig config = computationResult.config();
        Graph graph = computationResult.graph();

        AbstractResultBuilder<InfluenceMaximizationResult.Stats> builder = new InfluenceMaximizationResult.Stats.Builder()
                .withNodeCount( graph.nodeCount() )
                .withConfig( config )
                .withComputeMillis( computationResult.computeMillis() );

        return Stream.of( builder.build() );
    }

    @Override
    protected InfluenceMaximizationConfig newConfig(
            String username,
            Optional<String> graphName,
            Optional<GraphCreateConfig> maybeImplicitCreate,
            CypherMapWrapper config
    )
    {
        return new InfluenceMaximizationConfigImpl( graphName, maybeImplicitCreate, username, config );
    }

    @Override
    protected AlgorithmFactory<Greedy,InfluenceMaximizationConfig> algorithmFactory()
    {
        return (AlphaAlgorithmFactory<Greedy,InfluenceMaximizationConfig>) ( graph, configuration, tracker, log, eventTracker ) -> new Greedy(
                graph,
                configuration.k(),
                configuration.p(),
                configuration.mc(),
                Pools.DEFAULT,
                configuration.concurrency(),
                tracker
        );
    }
}
