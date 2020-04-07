package com.kafka.crawl.consumer;

import com.kafka.crawl.configuration.Configuration;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class CrawlWebsiteConsumer {

    private static Logger logger = LoggerFactory.getLogger(CrawlWebsiteConsumer.class);
    private static ExecutorService executorService = new ThreadPoolExecutor(100, 100, 0L,
            TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(100), new ThreadPoolExecutor.CallerRunsPolicy());

    public static void main(String[] args) throws Exception {
        try {
            Consumer<String, String> consumer = Configuration.createConsumer();
            new ConsumerData(consumer).start();
            //consumer.close();

        } catch (Exception e) {
            logger.error("Crawl Website Consumer Data Error = " + e.getMessage());
        }
    }


    private static class ConsumerData implements Runnable {
        private Thread thread;
        private Consumer<String, String> consumer;

        ConsumerData(Consumer<String, String> consumer) {
            this.consumer = consumer;
        }

        @Override
        public void run() {
            while (true) {

                final ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(3));

                System.out.println(consumerRecords);
                if (consumerRecords.count() > 0) {
                    consumerRecords.forEach(record -> {
                        executorService.submit(new Runnable() {
                            @Override
                            public void run() {
                                String urls = record.value();
                                System.out.println(urls);
                                try {
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            }
                        });
                    });
                }
            }
        }

        public void start() {
            if (thread == null) {
                thread = new Thread(this);
                thread.start();
            }
        }
    }
}