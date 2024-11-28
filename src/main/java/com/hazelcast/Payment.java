package com.hazelcast;

import com.hazelcast.Main.Classifier;
import com.hazelcast.vector.VectorValues;
import dev.langchain4j.model.embedding.AllMiniLmL6V2EmbeddingModel;
import dev.langchain4j.model.embedding.EmbeddingModel;
import org.apache.commons.lang3.RandomStringUtils;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

public class Payment implements Serializable {
    private static final EmbeddingModel embeddingModel = new AllMiniLmL6V2EmbeddingModel();
    private static final RandomStringUtils RANDOM = RandomStringUtils.insecure();
    private static final AtomicLong ID = new AtomicLong(1000);

    private final long paymentId;
    private final String sourceAccountNo;
    private final String targetAccountNo;
    private final String title;
    private long mandt;
    private String mandtname;
    private long lgnum;
    private long lqnum;

    public Payment(){
        this.paymentId = 0;
        this.sourceAccountNo = "";
        this.targetAccountNo = "";
        this.title = "";
        this.mandt = 0;
        this.mandtname = "";
        this.lgnum = 0;
        this.lqnum = 0;
    }

    public Payment(long paymentId, String sourceAccountNo, String targetAccountNo, String title) {
        this.paymentId = paymentId;
        this.sourceAccountNo = sourceAccountNo;
        this.targetAccountNo = targetAccountNo;
        this.title = title;
    }

    public Payment(String sourceAccountNo, String targetAccountNo, String title) {
        this(ID.incrementAndGet(), sourceAccountNo, targetAccountNo, title);
    }

    public VectorValues toVector() {
        return VectorValues.of(embeddingModel.embed(this.toString()).content().vector());
    }

    public static Payment generatePayment(Classifier classifier) {
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
        return String.format("%-5s | %-8s -> %-8s | %-8s | %-8s", paymentId, sourceAccountNo, targetAccountNo, title, mandtname);
    }

    // Getters for the fields
    public long getPaymentId() {
        return paymentId;
    }

    public String getSourceAccountNo() {
        return sourceAccountNo;
    }

    public String getTargetAccountNo() {
        return targetAccountNo;
    }

    public String getTitle() {
        return title;
    }
    public long getMandt() {
        return mandt;
    }

    public void setMandt(long mandt) {
        this.mandt = mandt;
    }
    public long getLgnum() {
        return lgnum;
    }

    public void setLgnum(long lgnum) {
        this.lgnum = lgnum;
    }

    public long getLqnum() {
        return lqnum;
    }

    public void setLqnum(long lqnum) {
        this.lqnum = lqnum;
    }
    public String getMandtname() {
        return mandtname;
    }

    public void setMandtname(String mandtname) {
        this.mandtname = mandtname;
    }

}
