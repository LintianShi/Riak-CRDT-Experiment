package record;

import java.util.ArrayList;
import java.util.List;

public class RiakOperation {
    private long startTime = 0;
    private long endTime = 0;
    private String operationName;
    private List<String> arguments;
    private String retValue = "none";

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

    public void setStartTime(long ts) {
        this.startTime = ts;
    }

    public void setEndTime(long ts) {
        this.endTime = ts;
    }

    public String toString() {
        String str = Long.toString(startTime) + "," + Long.toString(endTime) + "," + operationName;
        for (String arg : arguments) {
            str += "," + arg;
        }
        str += "," + retValue;
        return str;
    }
}
