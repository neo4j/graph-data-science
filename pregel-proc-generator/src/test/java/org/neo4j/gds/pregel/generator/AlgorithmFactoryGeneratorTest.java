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
package org.neo4j.gds.pregel.generator;

import com.squareup.javapoet.ClassName;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class AlgorithmFactoryGeneratorTest {

    private static final String NL = System.lineSeparator();

    @Test
    void shouldGenerateTypeSpec() {
        var typeNames = new TypeNames("gds.test", "Baz", ClassName.get("gds.testconfig", "TheConfig"));
        var generator = new AlgorithmFactoryGenerator(typeNames);
        var type = generator.typeSpec(Optional.empty());
        assertThat(type.toString()).isEqualTo("" +
            "public final class BazAlgorithmFactory extends org.neo4j.gds.GraphAlgorithmFactory<gds.test.BazAlgorithm, gds.testconfig.TheConfig> {" + NL +
            "}" + NL
        );
    }

    @Test
    void shouldGenerateBuildMethod() {
        var typeNames = new TypeNames("gds.test", "Baz", ClassName.get("gds.testconfig", "TheConfig"));
        var generator = new AlgorithmFactoryGenerator(typeNames);
        var type = generator.buildMethod();
        assertThat(type.toString()).isEqualTo("" +
            "@java.lang.Override" + NL +
            "public gds.test.BazAlgorithm build(org.neo4j.gds.api.Graph graph," + NL +
            "    gds.testconfig.TheConfig configuration," + NL +
            "    org.neo4j.gds.core.utils.progress.tasks.ProgressTracker progressTracker) {" + NL +
            "  return new gds.test.BazAlgorithm(graph, configuration, progressTracker);" + NL +
            "}" + NL
        );
    }

    @Test
    void shouldGenerateTaskNameMethod() {
        var typeNames = new TypeNames("gds.test", "Baz", ClassName.get("gds.testconfig", "TheConfig"));
        var generator = new AlgorithmFactoryGenerator(typeNames);
        var type = generator.taskNameMethod();
        assertThat(type.toString()).isEqualTo("" +
            "@java.lang.Override" + NL +
            "public java.lang.String taskName() {" + NL +
            "  return gds.test.BazAlgorithm.class.getSimpleName();" + NL +
            "}" + NL
        );
    }

    @Test
    void shouldGenerateProgressTaskMethod() {
        var typeNames = new TypeNames("gds.test", "Baz", ClassName.get("gds.testconfig", "TheConfig"));
        var generator = new AlgorithmFactoryGenerator(typeNames);
        var type = generator.progressTaskMethod();
        assertThat(type.toString()).isEqualTo("" +
            "@java.lang.Override" + NL +
            "public org.neo4j.gds.core.utils.progress.tasks.Task progressTask(org.neo4j.gds.api.Graph graph," + NL +
            "    gds.testconfig.TheConfig configuration) {" + NL +
            "  return Pregel.progressTask(graph, configuration);" + NL +
            "}" + NL
        );
    }

    @Test
    void shouldGenerateMemoryEstimationMethod() {
        var typeNames = new TypeNames("gds.test", "Baz", ClassName.get("gds.testconfig", "TheConfig"));
        var generator = new AlgorithmFactoryGenerator(typeNames);
        var type = generator.memoryEstimationMethod();
        assertThat(type.toString()).isEqualTo("" +
            "@java.lang.Override" + NL +
            "public org.neo4j.gds.core.utils.mem.MemoryEstimation memoryEstimation(" + NL +
            "    gds.testconfig.TheConfig configuration) {" + NL +
            "  var computation = new gds.test.Baz();" + NL +
            "  return org.neo4j.gds.beta.pregel.Pregel.memoryEstimation(computation.schema(configuration).propertiesMap(), computation.reducer().isEmpty(), configuration.isAsynchronous());" + NL +
            "}" + NL
        );
    }

}
