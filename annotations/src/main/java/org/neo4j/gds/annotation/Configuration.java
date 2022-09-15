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
package org.neo4j.gds.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Documented
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.CLASS)
public @interface Configuration {

    /**
     * Name of the generated class.
     * <p>
     * If not manually set, the value is set to the
     * annotation class name with an "Impl" suffix:
     *
     * <pre>
     * &#64;Configuration
     * interface Foo { }
     *
     * &#64;Generated
     * public class FooImpl { }
     * </pre>
     *
     */
    String value() default "";

    /**
     * By default, a configuration field is resolved in the {@link org.neo4j.gds.core.CypherMapWrapper} parameter with the method name as the expected key.
     * This annotation changes the key to look up to use {@link org.neo4j.gds.annotation.Configuration.Key#value()} instead.
     */
    @Documented
    @Target(ElementType.METHOD)
    @Retention(RetentionPolicy.CLASS)
    @interface Key {
        String value();
    }

    /**
     * This annotation can be used together with {@link org.neo4j.gds.annotation.Configuration.Key} or {@link org.neo4j.gds.annotation.Configuration.Parameter}.
     * The value must be a method reference of format `package.class#function` to a static and public method.
     * The input for the specific field will be transformed using the method-reference.
     */
    @Documented
    @Target(ElementType.METHOD)
    @Retention(RetentionPolicy.CLASS)
    @interface ConvertWith {
        String method();

        String INVERSE_IS_TO_MAP = "__USE_TO_MAP_METHOD__";

        // necessary if the ConvertWithMethod does not accept an already parsed value
        String inverse() default "";
    }

    /**
     * This annotation can be used together with {@link org.neo4j.gds.annotation.Configuration.Key} or {@link org.neo4j.gds.annotation.Configuration.Parameter}.
     * The value must be a method reference of format `package.class#function` to a static and public method.
     * The value of the specific field will be transformed using the method-reference and used for the implementation of the method annotated with {@link org.neo4j.gds.annotation.Configuration.ToMap}.
     */
    @Documented
    @Target(ElementType.METHOD)
    @Retention(RetentionPolicy.CLASS)
    @interface ToMapValue {
        String value();
    }

    /**
     * Used to specify which interface methods to ignore by the ConfigurationProcessor.
     */
    @Documented
    @Target(ElementType.METHOD)
    @Retention(RetentionPolicy.CLASS)
    @interface Ignore {
    }

    /**
     * Configuration field is expected to be passed to the constructor as a parameter.
     */
    @Documented
    @Target(ElementType.METHOD)
    @Retention(RetentionPolicy.CLASS)
    @interface Parameter {
        boolean acceptNull() default false;
    }

    /**
     * Annotated function will return the list of configuration keys.
     * The return type of the method must be of type Collection&lt;String&gt;.
     */
    @Documented
    @Target(ElementType.METHOD)
    @Retention(RetentionPolicy.CLASS)
    @interface CollectKeys {
    }

    /**
     * Annotated function will return the map representation of the configuration.
     * The return type of the method must be of type Map&lt;String, Object&gt;.
     *
     * By default, each field will be directly put into the returned map.
     * If {@link org.neo4j.gds.annotation.Configuration.ToMapValue} is defined, the given method will be applied before.
     */
    @Documented
    @Target(ElementType.METHOD)
    @Retention(RetentionPolicy.CLASS)
    @interface ToMap {
    }

    /**
     * The annotated method will be used to insert the implementation of validating a given graphStore.
     * The implementation calls each method annotated with {@link GraphStoreValidationCheck}.
     * <p>
     * The method cannot be abstract but should have an empty body, and have exactly three parameter graphStore, selectedLabels, selectedRelationshipTypes.
     */
    @Documented
    @Target(ElementType.METHOD)
    @Retention(RetentionPolicy.CLASS)
    @interface GraphStoreValidation {
    }

    /**
     * The annotated method will be used to insert the implementation of {@link org.neo4j.gds.annotation.Configuration.GraphStoreValidation} to verify the configuration is valid for the given graphStore.
     * <p>
     * The method cannot be abstract and must have exactly three parameters (graphStore, selectedLabels, selectedRelationshipTypes).
     * The method is expected to throw an exception if the check failed.
     */
    @Documented
    @Target(ElementType.METHOD)
    @Retention(RetentionPolicy.CLASS)
    @interface GraphStoreValidationCheck {
    }

    /**
     * Input for the annotated configuration field storing an Integer, will be validated if it is in the given range.
     */
    @Documented
    @Target(ElementType.METHOD)
    @Retention(RetentionPolicy.CLASS)
    @interface IntegerRange {
        int min() default Integer.MIN_VALUE;
        int max() default Integer.MAX_VALUE;
        boolean minInclusive() default true;
        boolean maxInclusive() default true;
    }

    /**
     * Input for the annotated configuration field storing a Long, will be validated if it is in the given range
     */
    @Documented
    @Target(ElementType.METHOD)
    @Retention(RetentionPolicy.CLASS)
    @interface LongRange {
        long min() default Long.MIN_VALUE;
        long max() default Long.MAX_VALUE;
        boolean minInclusive() default true;
        boolean maxInclusive() default true;
    }

    /**
     * Input for the annotated configuration field storing a Double, will be validated if it is in the given range
     */
    @Documented
    @Target(ElementType.METHOD)
    @Retention(RetentionPolicy.CLASS)
    @interface DoubleRange {
        double min() default -Double.MAX_VALUE;
        double max() default Double.MAX_VALUE;
        boolean minInclusive() default true;
        boolean maxInclusive() default true;
    }
}
