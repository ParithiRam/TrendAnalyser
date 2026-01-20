package com.antigravity.topic;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.bson.Document;

import java.time.Duration;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TopicExtractor {

    private static final String KAFKA_BROKER = "localhost:9092";
    private static final String INPUT_TOPIC = "raw-tweets";
    private static final String MONGO_URI = "mongodb://localhost:27017";
    private static final String DB_NAME = "trending_db";
    private static final String COLLECTION_NAME = "topics";

    public static void main(String[] args) {
        System.out.println("Starting Topic Extractor Service...");

        // Kafka Consumer Config
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKER);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "topic-extractor-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(INPUT_TOPIC));

        // MongoDB Config
        MongoClient mongoClient = MongoClients.create(MONGO_URI);
        MongoDatabase database = mongoClient.getDatabase(DB_NAME);
        MongoCollection<Document> collection = database.getCollection(COLLECTION_NAME);

        ObjectMapper mapper = new ObjectMapper();
        Map<String, Integer> topicFrequency = new HashMap<>();

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        JsonNode tweet = mapper.readTree(record.value());
                        String text = tweet.get("text").asText();
                        List<String> topics = extractTopics(text);

                        for (String topic : topics) {
                            topicFrequency.put(topic, topicFrequency.getOrDefault(topic, 0) + 1);
                            updateTopicInDb(collection, topic, topicFrequency.get(topic));
                        }
                        System.out.println("Processed tweet: " + text + " -> Topics: " + topics);

                    } catch (Exception e) {
                        System.err.println("Error processing record: " + e.getMessage());
                    }
                }
            }
        } finally {
            consumer.close();
            mongoClient.close();
        }
    }

    // Simple heuristic-based topic extraction for demo purposes
    // In a real production system, this would use LDA (Mallet) on a batch of documents.
    // Here we extract hashtags and capitalized keywords as "topics".
    private static List<String> extractTopics(String text) {
        List<String> topics = new ArrayList<>();
        
        // Extract hashtags
        Pattern hashtagPattern = Pattern.compile("#\\w+");
        Matcher hashtagMatcher = hashtagPattern.matcher(text);
        while (hashtagMatcher.find()) {
            topics.add(hashtagMatcher.group().substring(1)); // remove #
        }

        // Extract capitalized words (heuristic for proper nouns/keywords)
        String[] words = text.split("\\s+");
        for (String word : words) {
            if (word.length() > 3 && Character.isUpperCase(word.charAt(0)) && !word.startsWith("#")) {
                topics.add(word.replaceAll("[^a-zA-Z]", ""));
            }
        }
        return topics;
    }

    private static void updateTopicInDb(MongoCollection<Document> collection, String topic, int count) {
        Document query = new Document("name", topic);
        Document update = new Document("$set", new Document("count", count).append("last_updated", new Date()));
        collection.updateOne(query, update, new UpdateOptions().upsert(true));
    }
}
