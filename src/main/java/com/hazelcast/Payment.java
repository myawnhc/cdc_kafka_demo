package com.hazelcast;

import com.hazelcast.Main.Classifier;
import com.hazelcast.vector.VectorValues;
import dev.langchain4j.model.embedding.AllMiniLmL6V2EmbeddingModel;
import dev.langchain4j.model.embedding.EmbeddingModel;
import org.apache.commons.lang3.RandomStringUtils;

import java.util.concurrent.atomic.AtomicLong;

record Payment(long paymentId, String sourceAccountNo, String targetAccountNo, String title) {
    private static final EmbeddingModel embeddingModel = new AllMiniLmL6V2EmbeddingModel();
    private static final RandomStringUtils RANDOM = RandomStringUtils.insecure();
    private static final AtomicLong ID = new AtomicLong(1000);

     public Payment (String sourceAccountNo, String targetAccountNo, String title) {
         this(ID.incrementAndGet(), sourceAccountNo, targetAccountNo, title);
     }

    VectorValues toVector() {
        return VectorValues.of(embeddingModel.embed(this.toString()).content().vector());
    }

    static Payment generatePayment(Classifier classifier) {
        boolean legitSourceAddress = classifier == Classifier.LEGIT || Math.random() > 0.5;
        boolean legitTargetAddress = classifier == Classifier.LEGIT || Math.random() > 0.5;

        if (classifier == Classifier.SUSPICIOUS && legitSourceAddress && legitTargetAddress) {
            legitSourceAddress = false;
        }

        String sourceAccountNo = legitSourceAddress
                ? RANDOM.nextNumeric(10)
                : RANDOM.nextNumeric(5) + "mafia";
        String targetAccountNo = legitTargetAddress
                ? RANDOM.nextNumeric(10)
                : RANDOM.nextNumeric(5) + "mafia";
        return new Payment(ID.incrementAndGet(), sourceAccountNo, targetAccountNo,
                classifier == Classifier.LEGIT ? "pizza" : "extortion");
    }

    @Override
    public String toString() {
        return String.format("%-5s | %-8s -> %-8s | %-8s", paymentId, sourceAccountNo, targetAccountNo, title);
    }
}
