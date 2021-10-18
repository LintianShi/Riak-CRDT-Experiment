package generator;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import record.RiakOperation;

public abstract class ExpGenerator {
    private Random rand = new Random();
    private int totalOps;
    private String pattern;
    private AtomicInteger atomicInteger = new AtomicInteger(0);
    private List<RiakOperation> operations;

    public ExpGenerator(int totalOps, String pattern) {
        this.totalOps = totalOps;
        this.pattern = pattern;
        this.operations = new ArrayList<>(totalOps);
    }

    protected abstract RiakOperation generateOperation();

    public synchronized RiakOperation get() {
        int index = atomicInteger.incrementAndGet();
        if (index >= totalOps) {
            return null;
        }
        return operations.get(index);
    }

    protected void init() {
        for (int i = 0; i < totalOps; i++) {
            operations.add(generateOperation());
        }
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
