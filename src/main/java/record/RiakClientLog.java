package record;

import java.util.LinkedList;
import java.util.List;

import record.RiakOperation;

public class RiakClientLog {
    private List<RiakOperation> operationList = new LinkedList<>();

    public void appendLog(RiakOperation riakOperation) {
        operationList.add(riakOperation);
    }
}
