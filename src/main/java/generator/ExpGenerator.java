package generator;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import record.RiakOperation;

public abstract class ExpGenerator {
    private Random rand = new Random();
    private int totalOps;
    private String pattern;
    private AtomicInteger atomicInteger = new AtomicInteger(0);

    public ExpGenerator(int totalOps, String pattern) {
        this.totalOps = totalOps;
        this.pattern = pattern;
    }

    protected abstract RiakOperation generateOperation();

    public final RiakOperation generate() {
        atomicInteger.incrementAndGet();
        return generateOperation();
    }

    protected int randInt(int bound) {
        return rand.nextInt(bound);
    }

    public void setPattern(String pattern) {
        this.pattern = pattern;
    }

    public String getPattern() {
        return pattern;
    }

    public int getTotalOps() {
        return totalOps;
    }

    public void setTotalOps(int totalOps) {
        this.totalOps = totalOps;
    }

    public boolean isRunning() {
        return atomicInteger.get() < totalOps;
    }
}
