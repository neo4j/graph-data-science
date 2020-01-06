/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.SetMultimap;
import com.squareup.javapoet.JavaFile;
import org.neo4j.graphalgo.annotation.Configuration;

import javax.annotation.processing.Filer;
import javax.annotation.processing.Messager;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.tools.Diagnostic;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.util.Set;

public final class ConfigurationProcessingStep implements BasicAnnotationProcessor.ProcessingStep {

    private static final Class<Configuration> ANNOTATION_CLASS = Configuration.class;

    private final Messager messager;
    private final Filer filer;
    private final ConfigParser configParser;
    private final GenerateConfiguration generateConfiguration;

    ConfigurationProcessingStep(
        Messager messager,
        Filer filer,
        ConfigParser configParser,
        GenerateConfiguration generateConfiguration
    ) {
        this.messager = messager;
        this.filer = filer;
        this.configParser = configParser;
        this.generateConfiguration = generateConfiguration;
    }

    @Override
    public Set<? extends Class<? extends Annotation>> annotations() {
        return ImmutableSet.of(ANNOTATION_CLASS);
    }

    @Override
    public Set<? extends Element> process(SetMultimap<Class<? extends Annotation>, Element> elementsByAnnotation) {
        Set<Element> elements = elementsByAnnotation.get(ANNOTATION_CLASS);
        ImmutableSet.Builder<Element> elementsToRetry = ImmutableSet.builder();
        for (Element element : elements) {
            ProcessResult result = process(element);
            if (result == ProcessResult.RETRY) {
                elementsToRetry.add(element);
            }
        }
        return elementsToRetry.build();
    }

    private ProcessResult process(Element element) {
        if (element.getKind() != ElementKind.INTERFACE) {
            messager.printMessage(
                Diagnostic.Kind.ERROR,
                "The annotated configuration must be an interface.",
                element
            );
            return ProcessResult.INVALID;
        }
        ConfigParser.Spec configSpec = configParser.process(element.asType());
        Configuration configuration = element.getAnnotation(ANNOTATION_CLASS);
        JavaFile generatedConfig = generateConfiguration.generateConfig(configSpec, configuration.value());

        try {
            generatedConfig.writeTo(filer);
            return ProcessResult.PROCESSED;
        } catch (IOException e) {
            messager.printMessage(
                Diagnostic.Kind.ERROR,
                "Could not write config file: " + e.getMessage(),
                element
            );
            return ProcessResult.RETRY;
        }
    }

    enum ProcessResult {
        PROCESSED,
        INVALID,
        RETRY
    }
}
