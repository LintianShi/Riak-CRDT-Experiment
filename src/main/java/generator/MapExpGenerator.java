package generator;

import record.RiakOperation;
import generator.ExpGenerator;

public class MapExpGenerator extends ExpGenerator {
    private double proportionPut;
    private double proportionGet;
    private double proportionContainsValue;
    private double proportionSize;
    private int maxKey = 4;
    private int maxValue =Integer.MAX_VALUE;

    public MapExpGenerator(int totalOps) {
        super(totalOps, "default");
        proportionPut = 0.55;
        proportionGet = 0.15;
        proportionContainsValue = 0.15;
        proportionSize = 0.15;
        init();
    }

    public MapExpGenerator(int totalOps, String pattern) {
        super(totalOps, pattern);
        if (pattern.equals("ardominant")) {
            proportionPut = 0.55;
            proportionGet = 0.15;
            proportionContainsValue = 0.15;
            proportionSize = 0.15;
        } else {
            proportionPut = 0.60;
            proportionGet = 0.25;
            proportionContainsValue = 0.0;
            proportionSize = 0.15;
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
        } 
        // else if (random < proportionPut + proportionGet + proportionContainsValue) {
        //     operation = new RiakOperation("containsValue");
        //     int key = randInt(maxValue);
        //     operation.addArgument(Integer.toString(key));
        // } 
        else {
            operation = new RiakOperation("size");
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
