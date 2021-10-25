package record;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.LinkedList;
import java.util.List;

import record.RiakOperation;

public class RiakClientLog {
    private List<RiakOperation> operationList = new LinkedList<>();

    public void appendLog(RiakOperation riakOperation) {
        operationList.add(riakOperation);
    }

    public int size() {
        return operationList.size();
    }

    public void outputLog(BufferedWriter bw) throws Exception {
        for (RiakOperation operation : operationList) {
            bw.write("\n" + operation.toString());
        }
    }
}
