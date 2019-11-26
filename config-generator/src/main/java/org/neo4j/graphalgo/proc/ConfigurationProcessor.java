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

package org.neo4j.graphalgo.proc;

import com.google.auto.common.BasicAnnotationProcessor;
import com.google.auto.service.AutoService;

import javax.annotation.processing.Processor;
import javax.lang.model.SourceVersion;
import java.util.Collections;

@AutoService(Processor.class)
public final class ConfigurationProcessor extends BasicAnnotationProcessor {

    @Override
    public SourceVersion getSupportedSourceVersion() {
        SourceVersion sourceVersion = SourceVersion.latestSupported();
        // We don't require jdk11, but if we're on it or on a later one
        // support the jdk11 release
        if (sourceVersion.ordinal() > 11) {
            return SourceVersion.values()[11];
        }
        // Require at least jdk8
        if (sourceVersion.ordinal() < 8) {
            return SourceVersion.RELEASE_8;
        }
        return sourceVersion;
    }

    @Override
    protected Iterable<? extends ProcessingStep> initSteps() {
        ConfigParser configParser = new ConfigParser(processingEnv.getMessager());
        GenerateConfiguration generateConfiguration = new GenerateConfiguration(
            processingEnv.getMessager(),
            processingEnv.getElementUtils(),
            processingEnv.getTypeUtils(),
            getSupportedSourceVersion()
        );
        ProcessingStep configStep = new ConfigurationProcessingStep(
            processingEnv.getMessager(),
            processingEnv.getFiler(),
            configParser,
            generateConfiguration
        );
        return Collections.singleton(configStep);
    }
}
