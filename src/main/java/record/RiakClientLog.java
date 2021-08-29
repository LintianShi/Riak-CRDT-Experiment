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

    public void outputLog(String filename) throws Exception {
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(filename)));
        for (RiakOperation operation : operationList) {
            bw.write(operation.toString() + "\n");
        }
    }
}
