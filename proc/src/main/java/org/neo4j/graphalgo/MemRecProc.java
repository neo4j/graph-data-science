package org.neo4j.graphalgo;

import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.utils.ExceptionUtil;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.mem.MemoryTree;
import org.neo4j.graphalgo.impl.labelprop.LabelPropagation;
import org.neo4j.graphalgo.impl.results.MemRecResult;
import org.neo4j.graphdb.Direction;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.ProcedureHandle;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;

@SuppressWarnings("unused")
public final class MemRecProc {

    @Context
    public Log log;

    @Context
    public GraphDatabaseAPI api;

    @Context
    public KernelTransaction transaction;

    @Procedure(name = "algo.memrec", mode = READ)
    @Description("Calculates the memory requirements for the given graph and algo. " +
            "CALL algo.memrec(label:String, relationship:String, algo:String, " +
            "{weightProperty:'weight', concurrency:4, ...properties })" +
            "YIELD requiredMemory")
    public Stream<MemRecResult> memrec(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationship,
            @Name(value = "algo", defaultValue = "") String algo,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> configMap) {

        if (algo != null && !algo.isEmpty()) {
            String[] algoNamespace = {"algo"};
            QualifiedName name = new QualifiedName(Arrays.asList(algoNamespace), algo);
            Procedures procedures = api.getDependencyResolver().resolveDependency(Procedures.class);
            ProcedureHandle proc = null;
            try {
                proc = procedures.procedure(name);
            } catch (ProcedureException e) {
                if (!e.status().equals(Status.Procedure.ProcedureNotFound)) {
                    throw ExceptionUtil.asUnchecked(e);
                }
            }
            if (proc == null) {
                String available = procedures.getAllProcedures().stream()
                        .filter(p -> Arrays.equals(p.name().namespace(), algoNamespace))
                        .map(p -> p.name().name())
                        .distinct()
                        .collect(Collectors.joining(", ", "{", "}"));
                throw new IllegalArgumentException(String.format(
                        "No algo named [%s] found, the available algos are %s",
                        algo,
                        available));
            }

            String query = " CALL " + proc.signature().name() + ".memrec($label, $relationship, $config)";

            Stream.Builder<MemRecResult> builder = Stream.builder();
            api.execute(query, MapUtil.map(
                    "label", label,
                    "relationship", relationship,
                    "config", configMap
            )).accept(row -> {
                builder.add(new MemRecResult(row));
                return true;
            });
            return builder.build();
        }

        ProcedureConfiguration config = ProcedureConfiguration.create(configMap)
                .overrideNodeLabelOrQuery(label)
                .overrideRelationshipTypeOrQuery(relationship);
        MemoryTree memoryRequirements = gatherMemoryRequirements(config);
        return Stream.of(new MemRecResult(memoryRequirements));
    }

    // best effort guess at loading a graph with probably settings
    // since we don't have an algo to delegate to
    private MemoryTree gatherMemoryRequirements(ProcedureConfiguration config) {
        final Direction direction = config.getDirection(Direction.OUTGOING);
        final String nodeWeight = config.getString("nodeWeight", null);
        final String nodeProperty = config.getString("nodeProperty", null);
        boolean undirected = config.get("undirected", false);
        boolean sorted = config.get("sorted", false);

        GraphLoader graphLoader = new GraphLoader(api, Pools.DEFAULT)
                .init(log, config.getNodeLabelOrQuery(), config.getRelationshipOrQuery(), config)
                .withNodeStatement(config.getNodeLabelOrQuery())
                .withRelationshipStatement(config.getRelationshipOrQuery())
                .withOptionalRelationshipWeightsFromProperty(
                        config.getWeightProperty(),
                        config.getWeightPropertyDefaultValue(1.0))
                .withOptionalNodeProperty(nodeProperty, 0.0d)
                .withOptionalNodeWeightsFromProperty(nodeWeight, 1.0d)
                .withOptionalNodeProperties(
                        PropertyMapping.of(LabelPropagation.PARTITION_TYPE, nodeProperty, 0.0d),
                        PropertyMapping.of(LabelPropagation.WEIGHT_TYPE, nodeWeight, 1.0d)
                )
                .withDirection(direction)
                .withSort(sorted)
                .asUndirected(undirected);

        GraphFactory factory = graphLoader.build(config.getGraphImpl());
        GraphDimensions dimensions = factory.dimensions();
        int concurrency = config.getReadConcurrency();
        MemoryEstimation estimation = factory.memoryEstimation();
        return estimation.apply(dimensions, concurrency);
    }
}
