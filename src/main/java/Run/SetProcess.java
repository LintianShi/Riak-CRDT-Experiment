package Run;

import RiakClient.SetClient;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.Scanner;
import java.util.concurrent.CountDownLatch;

public class SetProcess implements Runnable {
        private File processFile;
        private CountDownLatch cdl;
        private String folderPath;
        private long commonStartTime;
        private SetClient setclient;

        public SetProcess(File file, CountDownLatch cdl, String fdPath, SetClient client, long cST){
            processFile = file;
            this.cdl = cdl;
            this.folderPath = fdPath;
            this.commonStartTime = cST;
            this.setclient = client;
        }

        public void run(){

//        System.out.println("Processing file: " + processFile.getName());
            String filename = processFile.getName().substring(0,processFile.getName().indexOf("."));
            String outputPath = this.folderPath + "/" + filename + "_" + System.nanoTime() + ".txt";
//        System.out.println(outputPath);
            try {
//                SetClient clusterRiakClient = new SetClient("127.0.0.1:8081,127.0.0.1:8082,127.0.0.1:8083");
                Scanner input = new Scanner(processFile);
                File outfile = new File(outputPath);
                PrintWriter output = new PrintWriter(outfile);
                while (input.hasNext()) {
//                    System.out.println("File: " + cFile.getName() + " Content: " + input.nextLine());
                    String cLine = input.nextLine();
                    String[] c = cLine.split(";"); //操作之间用;分割
                    long startTime = 0, endTime = 0;
                    for (int j = 0; j < c.length; j++) {
//                    System.out.println("parsing: " + c[j]);
                        String t = c[j].substring(0, c[j].indexOf("("));
                        if (t.equals("add")) {
//                            System.out.println("???");
                            String value = c[j].substring(c[j].indexOf("(") + 1, c[j].indexOf(")"));
                            startTime = System.nanoTime() - this.commonStartTime;
                            boolean addResult = setclient.setAdd(value);
                            endTime = System.nanoTime() - this.commonStartTime;
//                        System.out.println("add("+value+"):"+addResult);
                            output.print("add(" + value + "):" + addResult);
                        } else if (t.equals("remove")) {
//                            System.out.println("!!!");
                            String value = c[j].substring(c[j].indexOf("(") + 1, c[j].indexOf(")"));
                            startTime = System.nanoTime() - this.commonStartTime;
                            boolean removeResult = setclient.setRemove(value);
                            endTime = System.nanoTime() - this.commonStartTime;
//                        System.out.println("remove("+value+"):"+removeResult);
                            output.print("remove(" + value + "):" + removeResult);
                        } else if (t.equals("contains")) {
//                            System.out.println("...");
                            String value = c[j].substring(c[j].indexOf("(") + 1, c[j].indexOf(")"));
                            startTime = System.nanoTime() - this.commonStartTime;
                            boolean containsResult = setclient.setContains(value);
                            endTime = System.nanoTime() - this.commonStartTime;
//                        System.out.println("contains("+value+"):"+ containsResult);
                            output.print("contains(" + value + "):" + containsResult);
                        } else if (t.equals("size")){
                            startTime = System.nanoTime() - this.commonStartTime;
                            int sizeResult = setclient.setSize();
                            endTime = System.nanoTime() - this.commonStartTime;
                            output.print("size():" + sizeResult);
                        }
                        output.println(" Start_time:" + startTime + " End_time:" + endTime + " Last:" + (endTime - startTime));
                    }
                }
                input.close();
                output.close();
//            System.out.println("Finish processing-" + processFile.getName());
//                clusterRiakClient.close();
                cdl.countDown();
            } catch (FileNotFoundException e) {
                System.out.println("Cannot found your file: " + processFile.getName());
            } catch (Exception e) {

            }
        }

}
