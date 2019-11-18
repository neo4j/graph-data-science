package good;

import org.neo4j.graphalgo.annotation.Configuration;

public interface Inheritance {
    public interface BaseConfig {
        double inheritedValue();

        default short inheritedDefaultValue() {
            return 42;
        }

        int overriddenValue();

        default long overwrittenValue() {
            return 42;
        }

    }

    @Configuration("MyConfig")
    public interface MyConfig extends BaseConfig {
        String baseValue();

        @Override
        default int overriddenValue() {
            return 1337;
        }


        @Override
        default long overwrittenValue() {
            return 1337;
        }
    }
}
