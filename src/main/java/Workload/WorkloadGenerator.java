package Workload;

//要求：可以通过参数调控线程数、操作数

//需要通过输入参数指定测试的Data type类型：Set或Map

/*

Set类型的原子操作集合: add、remove、contains
Map类型的原子操作集合: put、get、remove、containsKey

Set类型的待测试操作集合: size、isEmpty、pollFirst、pollLast、toArray、toString
Map类型的待测试操作集合: contains(containsValue)、elements、entrySet、isEmpty、keySet、size、toString、values

 */

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.*;

public class WorkloadGenerator {

    //Parameters
    String targetType; //只有Set和Map两种
    Operation targetOp;
    String folderPath;
    int threadNum;
    int opNum;
    HashSet values;
    HashSet keySet;
    HashSet usedKey; //已经使用的key
    HashSet usedValue; //已经写入的值
    HashSet<String> atomicOps; //假定atomic的操作
    HashSet<String> readOnlyOps; //只读操作（不会改变数据状态）
    HashSet targetOps; //目标操作集合
    LinkedList<HashSet> ops; //所有操作的集合
    LinkedList<Integer> weightList;
    public ArrayList<Integer> weightTemp;
    int weightSum;

    public WorkloadGenerator(String type, String targetOp){
        this.targetType = type;
        if(type.equals("Set")){
            values = new HashSet();
            values.add("0");
            values.add("1");
            values.add("2");
            values.add("3");
            values.add("4");
            values.add("5");
            values.add("6");
            values.add("7");
            values.add("8");
            values.add("9");

            atomicOps = new HashSet<String>();
            atomicOps.add("add");
            atomicOps.add("remove");

            readOnlyOps= new HashSet<String>();
            readOnlyOps.add(targetOp);

            weightList = new LinkedList<Integer>();
            weightList.add(3);
            weightList.add(1);

            usedValue = new HashSet();

            ops = new LinkedList<HashSet>();

        }
        else if(type.equals("Map")){
            values = new HashSet();
            values.add("0");
            values.add("1");

            atomicOps = new HashSet<String>();
            atomicOps.add("put");
            atomicOps.add("get");
            atomicOps.add("remove");

            readOnlyOps= new HashSet<String>();
            readOnlyOps.add("containsKey");
            readOnlyOps.add(targetOp);

            weightList = new LinkedList<Integer>();
            weightList.add(4);
            weightList.add(1);

            usedValue = new HashSet();

            ops = new LinkedList<HashSet>();
        }
        threadNum = 3;
        opNum = 50;

    }

//    public workloadGenerator(String targetType, Operation targetOp, String folderPath, ){
//        this.targetType = targetType;
//        this.targetOp = targetOp;
//        this.folderPath = folderPath;
//
//    }

    public void prepare(){
        this.ops.add(this.atomicOps); // atomicOps对应着第一个weight
        this.ops.add(this.readOnlyOps); //对应着第二个weight
        this.weightTemp = new ArrayList<Integer>(weightList.size()+1);
        weightTemp.add(0);
        this.weightSum = 0;
        for(Integer i: weightList){
            this.weightSum += i;
            this.weightTemp.add(Integer.valueOf(this.weightSum));
        }
    }

    public void updateWeight(String newWeight){ //更新操作集合的权重
        String[] nwList = newWeight.split(",");
        this.weightList.clear();
        for(int i = 0; i < nwList.length; i++) {
            this.weightList.add(Integer.valueOf(nwList[i]));
        }
        this.weightTemp.clear();
        this.weightTemp.add(0);
        this.weightSum = 0;
        for(Integer i: weightList){
            this.weightSum += i;
            this.weightTemp.add(Integer.valueOf(this.weightSum));
        }
    }

    public void updateParas(int threadNum, int opNum, String folderPath){
        this.threadNum = threadNum;
        this.opNum = opNum;
        this.folderPath = folderPath;
    }

    public String generateName(){
        String opName = "";
        Random random = new Random();
        int rand = random.nextInt(this.weightSum);
        int index = 0;
        for(int i = this.weightTemp.size()-1; i > 0; i--){
            if(rand >= weightTemp.get(i)){
                index = i;
                break;
            }
        }
        HashSet<String> opSet = this.ops.get(index);
        rand = random.nextInt(opSet.size());
        Iterator it = opSet.iterator();
        int i = -1;
        while(i != rand && it.hasNext()){
            opName = (String)it.next();
            i++;
        }
        return opName;
    }


    public String generateOpfForSet(){
        String opName = this.generateName();
        Random random = new Random();
        int rand = random.nextInt(this.values.size());
        String opValue = "";
        Iterator it = this.values.iterator();
        int i = -1;
        while(i != rand && it.hasNext()){
            opValue = (String)it.next();
            i++;
        }
        return opName + "(" + opValue +");";
    }

    public String generateOpForMap(){
        String t = "";
        return t;
    }

    public void generateForSet(){
        LinkedList<String> threads = new LinkedList<String>();
        for(int i = 0; i < this.threadNum; i++){
            threads.add("Thread " + i + ":");
        }

        int j = 0;
        while(j < this.opNum){
            String op = this.generateOpfForSet(); //生成一个操作；
            Random random = new Random();
            int rand = random.nextInt(this.threadNum); //随机选取一个线程
            threads.set(rand, threads.get(rand).concat(op));
            j++;
        }
        for(int i = 0; i < threads.size(); i++){
            System.out.println(threads.get(i));
        }
    }

    public void generateForMap(){

    }

    public void generateForSetToFile(String folderPath) throws FileNotFoundException{
        Calendar cTime = Calendar.getInstance();
        String timeStamp = cTime.get(Calendar.YEAR) + "_" + (cTime.get(Calendar.MONTH)+1) + "_" + cTime.get(Calendar.DAY_OF_MONTH) +"_"+ cTime.get(Calendar.HOUR_OF_DAY);
        File folder = new File(folderPath +"/Running_"+timeStamp);
        folder.mkdirs();
        String commonOutPath = folder.getAbsolutePath();
        LinkedList<String> threads = new LinkedList<String>();
        for(int i = 0; i < this.threadNum; i++){
            threads.add("");

        }

        int j = 0;
        while(j < this.opNum){
            String op = this.generateOpfForSet(); //生成一个操作；
            Random random = new Random();
            int rand = random.nextInt(this.threadNum); //随机选取一个线程
            threads.set(rand, threads.get(rand).concat(op+" "));
            j++;
        }
        //将生成好的每一个线程对应的操作输出到文件中
        for(int i = 0; i < threads.size(); i++){
            String outputPath = commonOutPath + "/" +"p"+ i + ".txt";
            File outfile = new File(outputPath);
            PrintWriter output = new PrintWriter(outfile);
            String ops = threads.get(i);
            System.out.println(ops);
            String[] opList = ops.split(" ");
            for(j = 0; j < opList.length; j++){
                output.println(opList[j]);
            }
            output.close();
        }
    }

    public void generateForMapToFile(String folderPath) throws FileNotFoundException{

    }

    public String getFolderPath(){
        return this.folderPath;
    }


    public static void main(String args[]) throws Exception{


        WorkloadGenerator g = new WorkloadGenerator("Set", "contains");
        System.out.println("Test");
        System.out.println(g.atomicOps.toString());
        System.out.println(g.readOnlyOps.toString());
        g.prepare();
//        for(int i = 0; i < 10; i++){
//            System.out.print(g.generateOpfForSet() + " ");
//        }
//        g.generateForSet();
        g.generateForSetToFile("input");
    }

}
