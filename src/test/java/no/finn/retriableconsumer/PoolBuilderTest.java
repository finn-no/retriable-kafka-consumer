package no.finn.retriableconsumer;

import org.junit.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class PoolBuilderTest {

    @Test(expected = IllegalStateException.class)
    public void test_some() {
        new ReliablePoolBuilder<>(null).build();
    }

    @Test
    public void topic_name() {
        String retryTopic = ReliablePoolBuilder.retryTopicName("sometopic", "mygroup");
        assertThat(retryTopic).isEqualToIgnoringCase("retry-mygroup-sometopic");
    }

    @Test
    public void dont_prefix_if_already_prefixed() {
        String retryTopic = ReliablePoolBuilder.retryTopicName("retry-sometopic", "mygroup");
        assertThat(retryTopic).isEqualToIgnoringCase("retry-sometopic");
    }

}
