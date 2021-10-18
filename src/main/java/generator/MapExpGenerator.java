package generator;

import record.RiakOperation;
import generator.ExpGenerator;

public class MapExpGenerator extends ExpGenerator {
    private double proportionPut;
    private double proportionGet;
    private double proportionContainsValue;
    private int maxKey = 80;
    private int maxValue = 20;

    public MapExpGenerator(int totalOps) {
        super(totalOps, "default");
        proportionPut = 0.7;
        proportionGet = 0.15;
        proportionContainsValue = 0.15;
        init();
    }

    public MapExpGenerator(int totalOps, String pattern) {
        super(totalOps, pattern);
        if (pattern.equals("ardominant")) {
            proportionPut = 0.8;
            proportionGet = 0.1;
            proportionContainsValue = 0.1;
        } else {
            proportionPut = 0.7;
            proportionGet = 0.15;
            proportionContainsValue = 0.15;
        }
        init();
    }

    protected RiakOperation generateOperation() {
        double random = Math.random();
        RiakOperation operation = null;
        if (random < proportionPut) {
            operation = new RiakOperation("put");
            int key = randInt(maxKey);
            int value = randInt(maxValue);
            operation.addArgument(Integer.toString(key));
            operation.addArgument(Integer.toString(value));
        } else if (random < proportionPut + proportionGet) {
            operation = new RiakOperation("get");
            int key = randInt(maxKey);
            operation.addArgument(Integer.toString(key));
        } else {
            operation = new RiakOperation("containsValue");
            int key = randInt(maxKey);
            operation.addArgument(Integer.toString(key));
        }
        return operation;
    }

    public void setMaxKey(int maxKey) {
        this.maxKey = maxKey;
    }

    public void setMaxValue(int maxValue) {
        this.maxValue = maxValue;
    }

    public void setProportionGet(double proportionGet) {
        this.proportionGet = proportionGet;
    }

    public void setProportionPut(double proportionPut) {
        this.proportionPut = proportionPut;
    }

    public void setProportionContainsValue(double proportionContainsValue) {
        this.proportionContainsValue = proportionContainsValue;
    }

    public static void main(String[] args) {
        ExpGenerator generator = new MapExpGenerator(100);
    }

}
