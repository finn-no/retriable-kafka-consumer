package no.finntech.retriableconsumer;

import org.junit.Test;

public class PoolBuilderTest {

    @Test(expected = IllegalStateException.class)
    public void test_some(){
        new ReliablePoolBuilder<>(null).build();
    }
}
