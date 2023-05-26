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
package org.neo4j.gds.pregel;

import com.squareup.javapoet.ClassName;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.pregel.generator.TypeNames;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class AlgorithmGeneratorTest {

    private static final String NL = System.lineSeparator();

    @Test
    void shouldGenerateTypeSpec() {
        var typeNames = new TypeNames("a.b", "C", ClassName.get("d.e", "F"));
        var generator = new AlgorithmGenerator(typeNames);
        var spec = generator.typeSpec(Optional.empty());
        assertThat(spec.toString()).isEqualTo("" +
            "public final class CAlgorithm extends org.neo4j.gds.Algorithm<org.neo4j.gds.beta.pregel.PregelResult> {" + NL +
            "}" + NL
        );
    }

    @Test
    void shouldGeneratePregelJobField() {
        var typeNames = new TypeNames("a.b", "C", ClassName.get("d.e", "F"));
        var generator = new AlgorithmGenerator(typeNames);
        var spec = generator.pregelJobField();
        assertThat(spec.toString()).isEqualTo("" +
            "private final org.neo4j.gds.beta.pregel.Pregel<d.e.F> pregelJob;" + NL
        );
    }

    @Test
    void shouldGenerateConstructor() {
        var typeNames = new TypeNames("a.b", "C", ClassName.get("d.e", "F"));
        var generator = new AlgorithmGenerator(typeNames);
        var spec = generator.constructor();
        assertThat(spec.toString()).isEqualTo("" +
            "Constructor(org.neo4j.gds.api.Graph graph, d.e.F configuration," + NL +
            "    org.neo4j.gds.core.utils.progress.tasks.ProgressTracker progressTracker) {" + NL +
            "  super(progressTracker);" + NL +
            "  var computation = new a.b.C();" + NL +
            "  this.pregelJob = org.neo4j.gds.beta.pregel.Pregel.create(graph, configuration, computation, org.neo4j.gds.core.concurrency.Pools.DEFAULT, progressTracker);" + NL +
            "}" + NL
        );
    }

    @Test
    void shouldGenerateSetTerminationFlagMethod() {
        var typeNames = new TypeNames("a.b", "C", ClassName.get("d.e", "F"));
        var generator = new AlgorithmGenerator(typeNames);
        var spec = generator.setTerminatonFlag();
        assertThat(spec.toString()).isEqualTo("" +
            "@java.lang.Override" + NL +
            "public void setTerminationFlag(org.neo4j.gds.core.utils.TerminationFlag terminationFlag) {" + NL +
            "  super.setTerminationFlag(terminationFlag);" + NL +
            "  pregelJob.setTerminationFlag(terminationFlag);" + NL +
            "}" + NL
        );
    }

    @Test
    void shouldGenerateSetComputeMethod() {
        var typeNames = new TypeNames("a.b", "C", ClassName.get("d.e", "F"));
        var generator = new AlgorithmGenerator(typeNames);
        var spec = generator.computeMethod();
        assertThat(spec.toString()).isEqualTo("" +
            "@java.lang.Override" + NL +
            "public org.neo4j.gds.beta.pregel.PregelResult compute() {" + NL +
            "  return pregelJob.run();" + NL +
            "}" + NL
        );
    }
}
