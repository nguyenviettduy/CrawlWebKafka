package com.kafka.crawl.producer;

import com.kafka.crawl.configuration.Configuration;
import com.kafka.crawl.utils.Constant;
import com.kafka.crawl.utils.LoadProperties;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.util.HashSet;

public class CrawlWebsiteProducer {
    private static final int MAX_DEPTH = 2;
    private HashSet<String> links;

    public CrawlWebsiteProducer(){
        links = new HashSet<>();
    }

    public HashSet<String> getLinks() {
        return links;
    }

    public HashSet<String> getPageLinks(String URL, int depth){
        if(!links.contains(URL) && depth < MAX_DEPTH){
            System.out.println(">> Depth: " + depth + " [" + URL + "]");
            try {
                links.add(URL);

                Document document = Jsoup.connect(URL).get();
                Elements linksOnPage = document.select("a[href]");

                depth++;
                for (Element page : linksOnPage) {
                    getPageLinks(page.attr("abs:href"), depth);
                }
            } catch (IOException e) {
                System.err.println("For '" + URL + "': " + e.getMessage());
            }
        }
        return links;
    }

    public void sendMessage(HashSet<String> links){
        Producer<String, String> producer = Configuration.createProducer();
        for (String url : links) {
            producer.send(new ProducerRecord<String, String>(LoadProperties.getInstance()
                    .getProperties()
                    .getProperty(Constant.KAFKA_TOPIC), url));
        }
        System.out.println("Message sent successfully");
        producer.close();
    }

    public static void main(String[] args) {
        CrawlWebsiteProducer crawl = new CrawlWebsiteProducer();
        HashSet<String> links = crawl.getPageLinks("https://uet.vnu.edu.vn/", 0);
        crawl.sendMessage(links);
    }
}
