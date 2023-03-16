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
package org.neo4j.gds.collections.haa;

import org.neo4j.gds.collections.CollectionStep;
import org.neo4j.gds.collections.HugeAtomicArray;

import javax.annotation.processing.ProcessingEnvironment;
import java.nio.file.Path;

public final class HugeAtomicArrayStep extends CollectionStep<HugeAtomicArrayValidation.Spec> {

    private static final Class<HugeAtomicArray> HAA_ANNOTATION = HugeAtomicArray.class;

    public static HugeAtomicArrayStep of(ProcessingEnvironment processingEnv, Path sourcePath) {
        var validation = new HugeAtomicArrayValidation(
            processingEnv.getTypeUtils(),
            processingEnv.getElementUtils(),
            processingEnv.getMessager()
        );

        var mainGenerator = new HugeAtomicArrayGenerator();

        return new HugeAtomicArrayStep(processingEnv, sourcePath, validation, mainGenerator);
    }

    private HugeAtomicArrayStep(
        ProcessingEnvironment processingEnv,
        Path sourcePath,
        Validation<HugeAtomicArrayValidation.Spec> validation,
        Generator<HugeAtomicArrayValidation.Spec> mainGenerator
    ) {
        super(processingEnv, sourcePath, validation, mainGenerator, null);
    }

    @Override
    public String annotation() {
        return HAA_ANNOTATION.getCanonicalName();
    }
}
