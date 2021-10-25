package generator;

import record.RiakOperation;
import generator.ExpGenerator;

import java.util.ArrayList;
import java.util.List;

public class SetExpGenerator extends ExpGenerator {
    private double proportionAdd;
    private double proportionRemove;
    private double proportionContains;
    private double proportionSize;
    private int maxElement = 4;

    public SetExpGenerator(int totalOps) {
        super(totalOps, "default");
        proportionAdd = 0.3;
        proportionRemove = 0.3;
        proportionContains = 0.2;
        proportionSize = 0.2;
        init();
    }

    public SetExpGenerator(int totalOps, String pattern) {
        super(totalOps, pattern);
        if (pattern.equals("ardominant")) {
            proportionAdd = 0.3;
            proportionRemove = 0.3;
            proportionContains = 0.2;
            proportionSize = 0.2;
        } else {
            proportionAdd = 0.3;
            proportionRemove = 0.3;
            proportionContains = 0.2;
            proportionSize = 0.2;
        }
        init();
    }

    protected RiakOperation generateOperation() {
        double random = Math.random();
        RiakOperation operation = null;
        if (random < proportionAdd + proportionRemove + proportionContains) {
            int element = randInt(maxElement);
            if (random < proportionAdd) {
                operation = new RiakOperation("add");
            } else if (random < proportionAdd + proportionRemove) {
                operation = new RiakOperation("remove");
            } else {
                operation = new RiakOperation("contains");
            }
            operation.addArgument(Integer.toString(element));
        } else {
            operation = new RiakOperation("size");
        }
        return operation;
    }

    public void setProportionAdd(double proportionAdd) {
        this.proportionAdd = proportionAdd;
    }

    public void setProportionRemove(double proportionRemove) {
        this.proportionRemove = proportionRemove;
    }

    public void setProportionContains(double proportionContains) {
        this.proportionContains = proportionContains;
    }

    public void setMaxElement(int maxElement) {
        this.maxElement = maxElement;
    }

    public static void main(String[] args) {
        SetExpGenerator generator = new SetExpGenerator(100);
        generator.init();
    }
}
