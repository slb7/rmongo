package com.whatever.rmongo;

import com.mongodb.ConnectionString;
import com.mongodb.MongoTimeoutException;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import com.mongodb.reactivestreams.client.Success;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.mongodb.client.model.Filters.eq;


@SpringBootApplication
public class RmongoApplication {
    public static class OperationSubscriber<T> extends ObservableSubscriber <T> {

        @Override
        public void onSubscribe(final Subscription s) {
            super.onSubscribe(s);
            s.request(Integer.MAX_VALUE);
        }
    }

    public static class ObservableSubscriber<T> implements Subscriber <T> {
        private final List <T> received;
        private final List <Throwable> errors;

        private final CountDownLatch latch;
        private volatile Subscription subscription;
        private volatile boolean completed;

        ObservableSubscriber() {
            this.received = new ArrayList <T>();
            this.errors = new ArrayList <Throwable>();
            this.latch = new CountDownLatch(1);
        }

        @Override
        public void onSubscribe(final Subscription s) {
            subscription = s;
        }

        @Override
        public void onNext(final T t) {
            received.add(t);
        }

        @Override
        public void onError(final Throwable t) {
            errors.add(t);
            onComplete();
        }

        @Override
        public void onComplete() {
            completed = true;
            latch.countDown();
        }

        public Subscription getSubscription() {
            return subscription;
        }

        public List <T> getReceived() {
            return received;
        }

        public Throwable getError() {
            if (errors.size() > 0) {
                return errors.get(0);
            }
            return null;
        }

        public boolean isCompleted() {
            return completed;
        }

        public List <T> get(final long timeout, final TimeUnit unit) throws Throwable {
            return await(timeout, unit).getReceived();
        }

        public ObservableSubscriber <T> await() throws Throwable {
            return await(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        }

        public ObservableSubscriber <T> await(final long timeout, final TimeUnit unit) throws Throwable {
            subscription.request(Integer.MAX_VALUE);
            if (!latch.await(timeout, unit)) {
                throw new MongoTimeoutException("Publisher onComplete timed out");
            }
            if (!errors.isEmpty()) {
                throw errors.get(0);
            }
            return this;
        }
    }
//    @Bean
    CommandLineRunner n2() {
        return (String... args) -> {
            System.out.println("b2");
            ConnectionString cs = new ConnectionString("mongodb://ec2-18-209-62-109.compute-1.amazonaws.com");
            MongoClient mongoClient = MongoClients.create(/*cs*/);
            MongoDatabase database = mongoClient.getDatabase("temp");

            MongoCollection <Document> collection = database.getCollection("c1");

            ArrayList<Document> documents = new ArrayList <Document>();
            for(int i = 0;i < 1000000;i++) {
                documents.add(new Document("name", "x" + i).append("y",i));
                ObservableSubscriber<Success> success = new ObservableSubscriber <Success>();
                if(i % 10000 == 0 || i == 999999) {
                    collection.insertMany(documents).subscribe(success);
                    try {
                        success.await();
                    }
                    catch (Throwable t) {
                        System.out.println(t.getMessage());
                    }

                    documents.clear();
                }
            }
            System.out.println("done");
//            collection.count(
//                    new SingleResultCallback <Long>() {
//                        @Override
//                        public void onResult(final Long count, final Throwable t) {
//                            System.out.println(count);
//                        }
//                    });
            //QuickDocumentSubscriber sub = new QuickDocumentSubscriber(collection, new Random());
            //collection.find().first().subscribe(sub);

        };
    }
    @Bean
    CommandLineRunner nameless() {
        return (String... args) -> {
            System.out.println("hw");
//            ConnectionString cs = new ConnectionString("mongodb://ec2-18-209-62-109.compute-1.amazonaws.com");
            ConnectionString cs = new ConnectionString("mongodb://ec2-18-209-62-109.compute-1.amazonaws.com");
            MongoClient mongoClient = MongoClients.create(/*cs*/);
            MongoDatabase database = mongoClient.getDatabase("temp");

            MongoCollection <Document> collection = database.getCollection("c1");

//            collection.count(
//                    new SingleResultCallback <Long>() {
//                        @Override
//                        public void onResult(final Long count, final Throwable t) {
//                            System.out.println(count);
//                        }
//                    });
            QuickDocumentSubscriber sub = new QuickDocumentSubscriber(collection, new Random());
            collection.find().first().subscribe(sub);

        };
    }

    public static class QuickDocumentSubscriber extends OperationSubscriber <Document> {
        QuickDocumentSubscriber(MongoCollection<Document> collection, Random rand) {
            this.collection = collection;
            this.rand = rand;
        }
        MongoCollection<Document> collection;
        Random rand;
        int count = 0;
        Instant start = Instant.now();
        @Override
        public void onNext(final Document document) {
            super.onNext(document);
            String key = "x" + rand.nextInt(1000000);
            Document proj = Document.parse("{name:1,y:1,_id:0}");
            collection.find(eq("name", key)).projection(proj).subscribe(this);
            //Eyecatcher
            if(++count % 10000 == 0) {
                Duration d = Duration.between(start,Instant.now());
                long elapsed = d.toNanos() / 1000;
                long speed = elapsed / count;
                System.out.println(count + " " + d + ' ' + speed);

            }
            //System.out.println(document.toJson());
        }
    }
    public static class InsertSubscriber extends OperationSubscriber <Document> {
        InsertSubscriber() {
//            this.collection = collection;
//            this.rand = rand;
        }
        MongoCollection<Document> collection;
        Random rand;
        int count = 0;
        Instant start = Instant.now();
        @Override
        public void onNext(final Document document) {
            super.onNext(document);
//            String key = "x" + rand.nextInt(1000000);
//            collection.find(eq("_id", key)).first().subscribe(this);
//            if(++count % 10000 == 0) {
//                System.out.println(count + " " + Duration.between(start,Instant.now()));
//
//            }
            System.out.println(document.toJson());
        }
    }

    public static void main(String[] args) {
        SpringApplication.run(RmongoApplication.class, args);
        try {
            Thread.sleep(100000);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
}