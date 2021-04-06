package Workload;

/*
    暂时只支持put、get、contains操作
 */

public class Operation {

    public enum Type{
        put, get, contains, add, remove, isEmpty, size
    }

    Type type;
    int key;
    int value;
    int global_num; //由输入顺序所决定的操作全局序号（unique id)
    int process_num; //操作所在的线程编号
    int startTime;
    int endTime;
    String returnValue;

    public int getKey(){
        return this.key;
    }

    public Type getType(){
        return this.type;
    }

    public int getValue(){
        return this.value;
    }

    public int getGlobalNum(){
        return this.global_num;
    }

    public int getProcessNum(){
        return this.process_num;
    }

    public String getReturnValue(){return this.returnValue;}

    public int getStartTime() { return this.startTime; }

    public int getEndTime() { return this.endTime; }

    public void setStartTime(String startTime){
        this.startTime = Integer.valueOf(startTime);
    }

    public void setEndTime(String endTime){
        this.endTime = Integer.valueOf(endTime);
    }

    public void setReturnValue(String rvl){
        this.returnValue = rvl;
    }

    public Operation(String s, int gn, int pn, int stt, int edt, String rvl){ //根据输入序列初始化操作
        String t = s.substring(0, s.indexOf("("));
        if(t.equals("put")){
            this.type = Type.put;
            this.key = Integer.valueOf(s.substring(s.indexOf("(")+1, s.indexOf(",")));
            this.value = Integer.valueOf(s.substring(s.indexOf(",")+1, s.indexOf(")")));
        }
        else if(t.equals("get")){
            this.type = Type.get;
            this.key = Integer.valueOf(s.substring(s.indexOf("(")+1, s.indexOf(")")));
            this.value = -1;
        }
        else if(t.equals("contains")){
            this.type = Type.contains;
            this.key = -1;
            this.value = Integer.valueOf(s.substring(s.indexOf("(")+1, s.indexOf(")")));
        }
        else if(t.equals("add")){
            this.type = Type.add;
            this.value = Integer.valueOf(s.substring(s.indexOf("(")+1, s.indexOf(")")));
        }
        else if(t.equals("remove")){
            this.type = Type.remove;
            this.value = Integer.valueOf(s.substring(s.indexOf("(")+1, s.indexOf(")")));
        }
        else if(t.equals("isEmpty")){
            this.type = Type.isEmpty;
        }
        else if(t.equals("size")){
            this.type = Type.size;
        }

        this.global_num = gn;
        this.process_num = pn;
        this.startTime = stt;
        this.endTime = edt;
        this.returnValue = rvl;
    }

    public void initialFromLine(String opLine, int n_num, int p_num){

        String opString = opLine.substring(0,opLine.indexOf(")")+1);
        String[] opStrings = opLine.split(" ");
        if(opStrings.length > 1) { //包含起止时间的输入
            this.setInfo(opString, n_num, p_num);
            this.setStartTime(opStrings[1].substring(opStrings[1].indexOf(":") + 1));
            this.setEndTime(opStrings[2].substring(opStrings[2].indexOf(":") + 1));
            this.setReturnValue(opStrings[0].substring(opStrings[0].indexOf(":") + 1));
//            System.out.println("Adding " + opString + " to op list" + " start:" + startTime + " end:" + endTime);
        }
        else{ //只有操作信息的输入
            this.setInfo(opString, n_num, p_num);
        }
    }

    public void setInfo(String opString, int n_num, int p_num){ //设置操作的类型与键值信息
        String t = opString.substring(0, opString.indexOf("("));
        if(t.equals("put")){
            this.type = Type.put;
            this.key = Integer.valueOf(opString.substring(opString.indexOf("(")+1, opString.indexOf(",")));
            this.value = Integer.valueOf(opString.substring(opString.indexOf(",")+1, opString.indexOf(")")));
        }
        else if(t.equals("get")){
            this.type = Type.get;
            this.key = Integer.valueOf(opString.substring(opString.indexOf("(")+1, opString.indexOf(")")));
            this.value = -1;
        }
        else if(t.equals("contains")){
            this.type = Type.contains;
            this.key = -1;
            this.value = Integer.valueOf(opString.substring(opString.indexOf("(")+1, opString.indexOf(")")));
        }
        else if(t.equals("add")){
            this.type = Type.add;
            this.value = Integer.valueOf(opString.substring(opString.indexOf("(")+1, opString.indexOf(")")));
        }
        else if(t.equals("remove")){
            this.type = Type.remove;
            this.value = Integer.valueOf(opString.substring(opString.indexOf("(")+1, opString.indexOf(")")));
        }
        else if(t.equals("isEmpty")){
            this.type = Type.isEmpty;
        }
        else if(t.equals("size")){
            this.type = Type.size;
        }
        this.global_num = n_num;
        this.process_num = p_num;

    }


    public String printOp(){ //控制台输出当前operation的信息
        if(this.getType() == Type.put ){
            return "Type: " + this.getType() + " key: " + this.getKey() + " value: " + this.getValue() + " Global Num:" + this.getGlobalNum() + " Process Num: " + this.getProcessNum();
        }
        else if (this.getType() == Type.get){
            return "Type: " + this.getType() + " key: " + this.getKey() + " Global Num:" + this.getGlobalNum() + " Process Num: " + this.getProcessNum();
        }
        else if (this.getType() == Type.contains){
            return "Type: " + this.getType() + " value: " + this.getValue() + " Global Num:" + this.getGlobalNum() + " Process Num: " + this.getProcessNum();

        }
        return "false";
    }

    public String printBare(){ //控制台简要输出当前op
        if(this.getType() == Type.put){
            return "put("+this.getKey()+","+this.getValue()+")";
        }
        else if(this.getType() == Type.get){
            return "get("+this.getKey()+")";
        }
        else if (this.getType() == Type.contains){
            return "contains(" + this.getValue()+")";
        }
        else if (this.getType() == Type.add){
            return "add("+this.getValue()+")";
        }
        else if (this.getType() == Type.remove){
            return "remove("+this.getValue()+")";
        }
        else if (this.getType() == Type.isEmpty){
            return "isEmpty()";
        }
        else if (this.getType() == Type.size){
            return "size()";
        }
        return "false";

    }

    public boolean equalTo(Operation anotherOp){ //仅利用key和value判定op之间的相等关系
        if(this.type == anotherOp.type && this.key == anotherOp.key && this.value == anotherOp.value){
            return true;
        }
        else{
            return false;
        }
    }

}

