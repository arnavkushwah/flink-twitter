package com.arnav;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import twitter4j.conf.ConfigurationBuilder;

import java.util.Properties;

public class TwitterStreamingJob {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Set up Twitter API credentials
        Properties props = new Properties();
        props.setProperty(TwitterSource.CONSUMER_KEY, "your-consumer-key");
        props.setProperty(TwitterSource.CONSUMER_SECRET, "your-consumer-secret");
        props.setProperty(TwitterSource.TOKEN, "your-access-token");
        props.setProperty(TwitterSource.TOKEN_SECRET, "your-access-token-secret");

        // Create Twitter Source
        TwitterSource twitterSource = new TwitterSource(props);
        twitterSource.setCustomEndpointInitializer(new FilterEndpoint("Super Bowl"));

        // Stream tweets
        DataStream<String> stream = env.addSource(twitterSource);
        stream.print(); // Print tweets to console

        env.execute("Twitter Stream");
    }

    // Filters tweets based on a keyword
    public static class FilterEndpoint implements TwitterSource.EndpointInitializer {
        private final String keyword;

        public FilterEndpoint(String keyword) {
            this.keyword = keyword;
        }

        @Override
        public twitter4j.TwitterStream getEndpoint() {
            ConfigurationBuilder cb = new ConfigurationBuilder();
            cb.setJSONStoreEnabled(true);
            twitter4j.FilterQuery fq = new twitter4j.FilterQuery();
            fq.track(new String[]{keyword});
            return new twitter4j.TwitterStreamFactory(cb.build()).getInstance();
        }
    }
}