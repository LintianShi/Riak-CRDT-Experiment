package RiakClient;

import Run.SetProcess;
import Tools.FileAnalyzer;

import java.io.File;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.concurrent.CountDownLatch;

public class RiakClusters {
    public static void main(String [] args) throws Exception {
        SetClient clusterRiakClient = new SetClient("172.21.252.92:8087,172.21.252.91:8087,172.21.252.93:8087");
        String inputPath = "/home/shilintian/Running_1";
        Calendar cTime = Calendar.getInstance();
        String timeStamp = cTime.get(Calendar.YEAR) + "_" + (cTime.get(Calendar.MONTH)+1) + "_" + cTime.get(Calendar.DAY_OF_MONTH) +"_"+ cTime.get(Calendar.HOUR_OF_DAY)
                +cTime.get(Calendar.MINUTE);
        File folder = new File("/home/shilintian/Output_"+timeStamp);
        folder.mkdirs();
        String commonOutPath = folder.getAbsolutePath();
        ArrayList<File> fs= new ArrayList<File>();
        FileAnalyzer.getFiles(fs,inputPath);
        //p_num = fs.size();

        final CountDownLatch cdl = new CountDownLatch(fs.size()); //用于主线程等待子线程结束

        System.out.println("fs size:" + fs.size());
        long commonStartTime = System.nanoTime();
        LinkedList<Thread> threadList= new LinkedList<Thread>();
        for(int i = 0; i < fs.size(); i++){
            File cFile = fs.get(i);
            if(cFile.isFile()) {
                Runnable proc = new SetProcess(cFile,cdl,commonOutPath, clusterRiakClient, commonStartTime);
                Thread t = new Thread(proc);
                threadList.add(t);
            }
        }

        for(int i = 0; i < threadList.size(); i++){
            threadList.get(i).start();
            System.out.println("Running thread " + i);
        }

        cdl.await(); //等待所有子线程执行完毕后，回到主线程；

        System.out.println("Beginning clear");
        clusterRiakClient.setClear();
        clusterRiakClient.close();

        System.out.println("Finish Initialization!" );
    }
}
