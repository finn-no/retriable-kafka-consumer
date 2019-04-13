package no.finn.retriableconsumer.version;

import java.util.Locale;
import java.util.Optional;

import io.prometheus.client.Gauge;

public class ExposeVersion {

    private static Gauge gauge = Gauge
            .build()
            .name("retryable_kafkaconsumer_version")
            .labelNames("app")
            .help("version of retryable kafka consumer")
            .register();

    public static double init() {
        double version = version();
        gauge.labels(getApplicationNameForPrometheus()).set(version);
        return version;
    }

    private static double version() {
        try {
            return Double.parseDouble(ExposeVersion.class.getPackage().getSpecificationVersion());
        } catch (Exception e) {
            return -1.0;
        }
    }

    public static String getApplicationNameForPrometheus(){
        String key = "ARTIFACT_NAME";
        String original = Optional.ofNullable(System.getenv("ARTIFACT_NAME"))
                .orElseGet(() -> System.getProperty(key, "unknown-app"));

        return original.toLowerCase(Locale.ENGLISH).replaceAll("\\W", "_");
    }
}
