package bad;

import org.neo4j.graphalgo.annotation.Configuration;

@Configuration("InvalidConversionsMethodsConfig")
public interface InvalidConversionsMethods {

    @Configuration.ConvertWith("nonStatic")
    int foo1();

    @Configuration.Ignore // so that it isn't picked up as a config value
    default int nonStatic(String input) {
        return Integer.parseInt(input);
    }


    @Configuration.ConvertWith("generic")
    int foo2();

    static <A> A generic(String input) {
        return null;
    }


    @Configuration.ConvertWith("declaresThrows")
    int foo3();

    static int declaresThrows(String input) throws IllegalArgumentException {
        return Integer.parseInt(input);
    }


    @Configuration.ConvertWith("noParameters")
    int foo4();

    static int noParameters() {
        return Integer.parseInt(input);
    }


    @Configuration.ConvertWith("multipleParameters")
    int foo5();

    static int multipleParameters(String input, String input2) {
        return Integer.parseInt(input);
    }


    @Configuration.ConvertWith("invalidReturnType1")
    int foo6();

    static String invalidReturnType1(String input) {
        return input;
    }


    @Configuration.ConvertWith("invalidReturnType2")
    int foo7();

    static long invalidReturnType2(String input) {
        return Long.parseLong(input);
    }


    @Configuration.ConvertWith("invalidReturnType3")
    int foo8();

    static double invalidReturnType3(String input) {
        return Double.parseDouble(input);
    }


    @Configuration.ConvertWith("invalidReturnType4")
    int foo9();

    static void invalidReturnType4(String input) {
    }


    @Configuration.ConvertWith("bad.InvalidConversionsMethods.Inner#privateMethod")
    int foo10();

    @Configuration.ConvertWith("bad.InvalidConversionsMethods.Inner#packagePrivateMethod")
    int foo11();

    abstract class Inner {
        private static int privateMethod(String input) {
            return Integer.parseInt(input);
        }

        static int packagePrivateMethod(String input) {
            return Integer.parseInt(input);
        }
    }
}
