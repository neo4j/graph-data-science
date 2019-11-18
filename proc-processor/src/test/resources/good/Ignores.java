package good;

import org.neo4j.graphalgo.annotation.Configuration;

import java.util.Collections;
import java.util.Map;

public interface Ignores {

    public interface BaseConfig {
        double canAlsoIgnoredInheritedMethods();

        long notIgnored();
    }

    @Configuration("MyConfig")
    public interface MyConfig extends BaseConfig {

        @Configuration.Ignore
        default String canIgnoreAnyMethod() {
            return "foo";
        }

        @Configuration.Ignore
        @Override
        default double canAlsoIgnoredInheritedMethods() {
            return 42;
        }

        static int staticMethodsAreAlsoIgnored() {
            return 1337;
        }

        @Configuration.Ignore
        @Configuration.Parameter
        default String canIgnoreParametersAsWell() {
            return "foo";
        }

        @Configuration.Ignore
        @Configuration.Key("bar")
        default String canIgnoreKeyAnnotationsAsWell() {
            return "baz";
        }

        @Configuration.Ignore
        default char canIgnoreInvalidMethods() {
            return 's';
        }

        @Configuration.Ignore
        default void canIgnoreInvalidMethods2() {}

        @Configuration.Ignore
        default int[] canIgnoreInvalidMethods3() {
            return new int[42];
        }

        @Configuration.Ignore
        default String canIgnoreInvalidMethods4(boolean nope) {
            return "foo";
        }

        @Configuration.Ignore
        default <A> A canIgnoreInvalidMethods5() {
            return null;
        }

        @Configuration.Ignore
        default <V> Map<String, V> canIgnoreInvalidMethods6() {
            return Collections.emptyMap();
        }

        @Configuration.Ignore
        default int canIgnoreInvalidMethods7() throws Exception {
            return 42;
        }

        @Configuration.Ignore
        default String canIgnoreInvalidMethods8() throws IllegalArgumentException {
            return "bar";
        }
    }
}
