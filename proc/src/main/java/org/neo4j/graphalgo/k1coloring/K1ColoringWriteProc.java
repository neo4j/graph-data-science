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

package org.neo4j.graphalgo.k1coloring;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.graphalgo.core.write.NodePropertyExporter;
import org.neo4j.graphalgo.impl.coloring.K1Coloring;
import org.neo4j.graphalgo.newapi.GraphCreateConfig;
import org.neo4j.graphalgo.result.AbstractResultBuilder;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

public class K1ColoringWriteProc extends K1ColoringBaseProc<K1ColoringWriteConfig> {
    private static final String COLOR_COUNT_FIELD_NAME = "colorCount";

    @Procedure(name = "algo.beta.k1coloring.write", mode = Mode.WRITE)
    @Description(DESCRIPTION)
    public Stream<WriteResult> betaK1Coloring(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ComputationResult<K1Coloring, K1Coloring, K1ColoringWriteConfig> compute = compute(
            graphNameOrConfig,
            configuration
        );
        K1ColoringWriteConfig config = compute.config();
        WriteResultBuilder builder = new WriteResultBuilder(config);
        K1Coloring coloring = compute.result();
        if (coloring == null) {
            return Stream.empty();
        }

        if (callContext.outputFields().anyMatch((field) -> field.equals(COLOR_COUNT_FIELD_NAME))) {
            builder.withColorCount(coloring.usedColors().cardinality());
        }

        builder
            .withWriteProperty(config.writeProperty())
            .withRanIterations(coloring.ranIterations())
            .withDidConverge(coloring.didConverge());

        HugeLongArray colors = coloring.colors();
        TerminationFlag terminationFlag = TerminationFlag.wrap(transaction);
        write(compute.graph(), colors, config.writeProperty(), builder, terminationFlag, config.writeConcurrency());

        return Stream.of(builder.build());
    }

    private void write(
        Graph graph,
        HugeLongArray coloring,
        String writeProperty,
        WriteResultBuilder resultBuilder,
        TerminationFlag terminationFlag,
        int writeConcurrency
    ) {
        log.debug("Writing results");

        NodePropertyExporter exporter = NodePropertyExporter.of(api, graph, terminationFlag)
            .withLog(log)
            .parallel(Pools.DEFAULT, writeConcurrency)
            .build();
        exporter.write(
            writeProperty,
            coloring,
            HugeLongArray.Translator.INSTANCE
        );
        resultBuilder.withNodePropertiesWritten(exporter.propertiesWritten());
    }

    @Override
    protected K1ColoringWriteConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return K1ColoringWriteConfig.of(username, graphName, maybeImplicitCreate, config);
    }

    public static class WriteResultBuilder extends AbstractResultBuilder<K1ColoringWriteConfig, WriteResult> {

        private long colorCount = -1L;
        private long ranIterations;
        private boolean didConverge;
        private String writeProperty;

        WriteResultBuilder(K1ColoringWriteConfig config) {
            super(config);
        }

        public WriteResultBuilder withColorCount(long colorCount) {
            this.colorCount = colorCount;
            return this;
        }

        WriteResultBuilder withRanIterations(long ranIterations) {
            this.ranIterations = ranIterations;
            return this;
        }

        WriteResultBuilder withDidConverge(boolean didConverge) {
            this.didConverge = didConverge;
            return this;
        }

        WriteResultBuilder withWriteProperty(String writeProperty) {
            this.writeProperty = writeProperty;
            return this;
        }

        @Override
        public WriteResult build() {
            return new WriteResult(
                createMillis,
                computeMillis,
                writeMillis,
                nodePropertiesWritten,
                colorCount,
                ranIterations,
                true,
                didConverge,
                writeProperty
            );
        }
    }

//    public Stream<K1ColoringProc.WriteResult> run(String label, String relationshipType, Map<String, Object> config) {
//        K1ColoringProc.ProcedureSetup setup = setup(label, relationshipType, config);
//
//        if (setup.graph.isEmpty()) {
//            setup.graph.release();
//            return Stream.of(K1ColoringProc.WriteResult.EMPTY);
//        }
//
//        K1Coloring coloring = compute(setup);
//
//        if (callContext.outputFields().anyMatch((field) -> field.equals(COLOR_COUNT_FIELD_NAME))) {
//            setup.builder.withColorCount(coloring.usedColors().cardinality());
//        }
//
//        Optional<String> writeProperty = setup.procedureConfig.getString(WRITE_PROPERTY_KEY);
//
//        setup.builder
//            .withRanIterations(coloring.ranIterations())
//            .withDidConverge(coloring.didConverge());
//
//        if (setup.procedureConfig.isWriteFlag() && writeProperty.isPresent() && !writeProperty.get().equals("")) {
//            setup.builder.withWriteProperty(writeProperty.get());
//            write(
//                setup.builder,
//                setup.graph,
//                coloring.colors(),
//                setup.procedureConfig,
//                writeProperty.get(),
//                coloring.terminationFlag,
//                setup.tracker
//            );
//
//            setup.graph.releaseProperties();
//        }
//
//        return Stream.of(setup.builder.build());
//    }


    public static class WriteResult {

        public static final WriteResult EMPTY = new WriteResult(
            0,
            0,
            0,
            0,
            0,
            0,
            false,
            false,
            null
        );

        public final long loadMillis;
        public final long computeMillis;
        public final long writeMillis;

        public final long nodes;
        public final long colorCount;
        public final long ranIterations;
        public final boolean didConverge;
        public final String writeProperty;

        public final boolean write;

        public WriteResult(
            long loadMillis,
            long computeMillis,
            long writeMillis,
            long nodes,
            long colorCount,
            long ranIterations,
            boolean write,
            boolean didConverge,
            String writeProperty
        ) {
            this.loadMillis = loadMillis;
            this.computeMillis = computeMillis;
            this.writeMillis = writeMillis;
            this.nodes = nodes;
            this.colorCount = colorCount;
            this.ranIterations = ranIterations;
            this.write = write;
            this.didConverge = didConverge;
            this.writeProperty = writeProperty;
        }
    }

}
