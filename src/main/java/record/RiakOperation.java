package record;

import java.util.ArrayList;
import java.util.List;

public class RiakOperation {
    private String operationName;
    private List<String> arguments;
    private String retValue;

    public RiakOperation() {
        arguments = new ArrayList<>();
    }

    public RiakOperation(String operationName) {
        arguments = new ArrayList<>();
        this.operationName = operationName;
    }

    public String getOperationName() {
        return operationName;
    }

    public String getRetValue() {
        return retValue;
    }

    public List<String> getArguments() {
        return arguments;
    }

    public void setRetValue(String retValue) {
        this.retValue = retValue;
    }

    public void addArgument(String argument) {
        this.arguments.add(argument);
    }

    public String toString() {
        return operationName + ", " + arguments.toString() + ", " + retValue;
    }
}
