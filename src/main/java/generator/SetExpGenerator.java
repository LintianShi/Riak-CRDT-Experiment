package generator;

import record.RiakOperation;
import generator.ExpGenerator;

import java.util.Random;

public class SetExpGenerator extends ExpGenerator {
    private double proportionAdd;
    private double proportionRemove;
    private double proportionContains;

    public SetExpGenerator(int totalOps) {
        super(totalOps, "default");
        proportionAdd = 0.6;
        proportionRemove = 0.3;
        proportionContains = 0.1;
    }

    public SetExpGenerator(int totalOps, String pattern) {
        super(totalOps, pattern);
        if (pattern.equals("ardominant")) {
            proportionAdd = 0.3;
            proportionRemove = 0.5;
            proportionContains = 0.2;
        } else {
            proportionAdd = 0.6;
            proportionRemove = 0.3;
            proportionContains = 0.1;
        }
    }

    protected RiakOperation generateOperation() {
        double random = Math.random();
        RiakOperation operation = null;
        if (random < proportionAdd) {
            operation = new RiakOperation("add");
        } else if (random < proportionAdd + proportionRemove) {
            operation = new RiakOperation("remove");
        } else {
            operation = new RiakOperation("contains");
        }

        int element = randInt();
        operation.addArgument(Integer.toString(element));
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

    public static void main(String[] args) {
        SetExpGenerator generator = new SetExpGenerator(100);
        System.out.println(generator.generate());
        System.out.println(generator.generate());
        System.out.println(generator.generate());
    }
}
