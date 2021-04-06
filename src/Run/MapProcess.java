package Run;


import RiakClient.MapClient;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.*;
import java.util.concurrent.CountDownLatch;

public class MapProcess implements Runnable{

    private File processFile;
    private CountDownLatch cdl;
    private String folderPath;
    private long commonStartTime;

    public MapProcess(File file, CountDownLatch cdl, String fdPath, long cST){
        processFile = file;
        this.cdl = cdl;
        this.folderPath = fdPath;
        this.commonStartTime = cST;
    }

    public void run(){

        System.out.println("Processing file: " + processFile.getName());
//        String outputPath = "/Users/dh/TestZone/preComputing/output";
        String filename = processFile.getName().substring(0,processFile.getName().indexOf("."));
        String outputPath = this.folderPath + "/" + filename +"_" +System.nanoTime() +".txt";
        System.out.println(outputPath);
        try {
            MapClient clusterRiakClient = new MapClient("127.0.0.1:8081,127.0.0.1:8082,127.0.0.1:8083");
            Scanner input = new Scanner(processFile);
            File outfile = new File(outputPath);
            PrintWriter output = new PrintWriter(outfile);
            while (input.hasNext()) {
//                    System.out.println("File: " + cFile.getName() + " Content: " + input.nextLine());
                String cLine = input.nextLine();
                String[] c = cLine.split(";"); //操作之间用;分割
                long startTime =0, endTime = 0;
                for (int j = 0; j < c.length; j++) {
//                    System.out.println("parsing: " + c[j]);
                    String t = c[j].substring(0, c[j].indexOf("("));
                    if(t.equals("put")){
                        String key = c[j].substring(c[j].indexOf("(")+1, c[j].indexOf(","));
                        String value = c[j].substring(c[j].indexOf(",")+1, c[j].indexOf(")"));
                        startTime = System.nanoTime()-this.commonStartTime;
                        String putResult = clusterRiakClient.mapPut(key, value);
                        endTime = System.nanoTime()-this.commonStartTime;
                        System.out.println("put("+key+","+value+"):"+putResult);
                        output.print("put("+key+","+value+"):"+putResult);
                    }
                    else if (t.equals("get")){
                        String key = c[j].substring(c[j].indexOf("(")+1, c[j].indexOf(","));
                        startTime = System.nanoTime()-this.commonStartTime;
                        String getResult = clusterRiakClient.mapGet(key);
                        endTime = System.nanoTime()-this.commonStartTime;
                        System.out.println("get("+key+"):"+getResult);
                        output.print("get("+key+"):"+getResult);
                    }
                    else if(t.equals("contains")){
                        String value = c[j].substring(c[j].indexOf("(")+1, c[j].indexOf(")"));
                        startTime = System.nanoTime()-this.commonStartTime;
                        boolean containsResult = clusterRiakClient.mapContainsValue(value);
                        endTime = System.nanoTime()-this.commonStartTime;
                        System.out.println("contains("+value+"):"+ containsResult);
                        output.print("contains("+value+"):"+ containsResult);
                    }
                    output.println(" Start_time:" + startTime +" End_time:" + endTime + " Last:" + (endTime-startTime));
                    Random rand = new Random();
//                    Thread.sleep(rand.nextInt(100));
//                        Operation op = new Operation(c[j], n_num, p_num);
//                        po.addOp(op);
//                        ops.add(op);
//                        System.out.println(op.printOp());
//                    n_num++;
                }
            }
            input.close();
            output.close();
            System.out.println("Finish processing-" + processFile.getName());
            cdl.countDown();
            clusterRiakClient.close();
        }
        catch (FileNotFoundException e){
            System.out.println("Cannot found your file: " + processFile.getName());
        }
        catch (Exception e){

        }

    }

}