package org.vanilladb.core.util;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException; 
import java.io.File;
import java.io.*;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
/*
 * This script is used to process the data for experiments. 
 */
public class dataProcessing {
    private int MISSING = -2147483648;
    public static final int hashPrime = 1572869;
    public static final int occupancyCard = 194400;//11480
    public static final int locationMIN = 1, locationMAX = 3000;//1000,3000
    List<Integer> roomPool = new ArrayList<>();
    List<Integer> macPool = new ArrayList<>();

    List<occupancyTuple> occupancyTuples = new ArrayList<>();
    List<wifiTuple> wifiTuples = new ArrayList<>();
    List<userTuple> userTuples = new ArrayList<>();

    static class wifiTuple{
        int st, et, mac, lid, duration;
        public wifiTuple(int st, int et, int mac, int lid, int duration){
            this.st = st;
            this.et = et;
            this.mac = mac;
            this.lid = lid;
            this.duration = duration;
        }

        public List<Integer> getList(){
            List<Integer> values = new ArrayList<>();
            values.add(st);
            values.add(et);
            values.add(mac);
            values.add(lid);
            values.add(duration);
            return values;
        }

        public int getSt() {
            return st;
        }

        public void setSt(int st) {
            this.st = st;
        }

        public int getEt() {
            return et;
        }

        public void setEt(int et) {
            this.et = et;
        }

        public int getMac() {
            return mac;
        }

        public void setMac(int mac) {
            this.mac = mac;
        }

        public int getLid() {
            return lid;
        }

        public void setLid(int lid) {
            this.lid = lid;
        }

        public int getDuration() {
            return duration;
        }

        public void setDuration(int duration) {
            this.duration = duration;
        }

        public String toString(){
            //st, et, mac, lid, duration
            String out = "";
            out += st + "," + et + "," + mac + "," + lid + "," + duration;
            return out;
        }
    }

    static class userTuple{
        int mac, name, email, group;
        public userTuple(int mac, int name, int email, int group){
            this.mac = mac;
            this.name = name;
            this.email = email;
            this.group = group;
        }

        public List<Integer> getList(){
            List<Integer> values = new ArrayList<>();
            values.add(this.mac);
            values.add(this.name);
            values.add(this.email);
            values.add(this.group);
            return values;
        }

        public int getMac() {
            return mac;
        }

        public void setMac(int mac) {
            this.mac = mac;
        }

        public int getName() {
            return name;
        }

        public void setName(int name) {
            this.name = name;
        }

        public int getEmail() {
            return email;
        }

        public void setEmail(int email) {
            this.email = email;
        }

        public int getGroup() {
            return group;
        }

        public void setGroup(int group) {
            this.group = group;
        }

        public String toString(){
            String out = "";
            out += mac + "," + name + "," + email + "," + group;
            return out;
        }
    }

    static class occupancyTuple{
        int lid, st,et,occupancy, type;
        public occupancyTuple(int lid, int st, int et, int occupancy, int type){
            this.lid = lid;
            this.st = st;
            this.et = et;
            this.occupancy = occupancy;
            this.type = type;
        }

        public List<Integer> getList() {
            List<Integer> values = new ArrayList<>();
            values.add(this.lid);
            values.add(this.st);
            values.add(this.et);
            values.add(this.occupancy);
            values.add(this.type);
            return values;
        }

        public int getLid() {
            return lid;
        }

        public void setLid(int lid) {
            this.lid = lid;
        }

        public int getSt() {
            return st;
        }

        public void setSt(int st) {
            this.st = st;
        }

        public int getEt() {
            return et;
        }

        public void setEt(int et) {
            this.et = et;
        }

        public int getOccupancy() {
            return occupancy;
        }

        public void setOccupancy(int occupancy) {
            this.occupancy = occupancy;
        }

        public int getType() {
            return type;
        }

        public void setType(int type) {
            this.type = type;
        }

        public String toString(){
            //lid, st,et,occupancy, type;
            String out = "";
            out = lid + "," + st + "," + et + "," + occupancy + "," + type;
            return out;
        }
    }

    public void readusers(){
        String csvFile = "/Users/yiminglin/Documents/Codebase/filter_optimization/data/smartbench/users.txt";
        String[] values;
        String delimiter = ",";
        try{
            File file = new File(csvFile);
            FileReader fr = new FileReader(file);
            BufferedReader br = new BufferedReader(fr);
            String line = "";
            int mac=0, name=0, email=0, group=0;
            while((line = br.readLine()) != null) {
                values = line.split(delimiter);
                for (int i = 0; i < values.length; i++){
                    String val = values[i];
                    if(i==0){
                        mac = Integer.valueOf(val);
                    }
                    if(i==1){
                        name = Integer.valueOf(val);
                    }
                    if(i==2){
                        email = Integer.valueOf(val);
                    }
                    if(i==3){
                        group = Integer.valueOf(val);
                    }
                }
                userTuples.add(new userTuple(mac, name, email, group));
            }
            br.close();
        }catch(IOException ioe) {
               ioe.printStackTrace();
        }
    }

    public void processUsers(){
        String fileName = "/Users/yiminglin/Documents/Codebase/filter_optimization/data/smartbench/usersClean.txt";
        File file = new File(fileName); 
        readMacs();
        try {
            FileWriter out = new FileWriter(file);
            BufferedWriter bw=new BufferedWriter(out);

            for(int i=0;i<userTuples.size();i++){
                userTuple t = userTuples.get(i);
                if(t.getMac() == MISSING){
                    t.setMac(setMac());
                }
                if(t.getGroup() == MISSING){
                    t.setGroup(setGroup());
                }
                bw.write(t.toString());
                bw.newLine();
            }
            
            bw.flush();
            bw.close();
            }catch (IOException e) {e.printStackTrace();}
    }

    

    public int rehash(int val){
                return val % hashPrime;
            }

    public int setLocation(){
        return ThreadLocalRandom.current().nextInt(locationMIN, locationMAX);
    }

    public int setGroup(){
        return ThreadLocalRandom.current().nextInt(1, 8);
    }

    public int setType(){
        Random rand = new Random();
        int coin = rand.nextInt(100);
        int type;
        if(coin < 60){
            type = 1;
        }else if(coin < 80){
            type = 2;
        }else if(coin < 95){
            type = 3;
        }else{
            type = 4;
        }
        return type;
    }

    public int setOccupancy(int type){
        int occupancy = 0;
        if(type == 1){//room
            occupancy = ThreadLocalRandom.current().nextInt(0,20);
        }
        else if(type == 2){//region
            occupancy = ThreadLocalRandom.current().nextInt(10,50);
        }
        else if(type == 3){//floor
            occupancy = ThreadLocalRandom.current().nextInt(40,100);
        }
        else{//4:building
            occupancy = ThreadLocalRandom.current().nextInt(70,300);
        }
        return occupancy;
    }

    public int setMac(){
        int num = ThreadLocalRandom.current().nextInt(0, macPool.size());
        return rehash(macPool.get(num));
    }

    public void readMacs(){
        String fileSpace = "/Users/yiminglin/Documents/Codebase/QDMIDB/QDMIDB/qdmidb/simpledb/wifidataset/wifiClean.txt";
        try{
            BufferedReader csvReader = new BufferedReader(new FileReader(fileSpace));
            int count = 0;
            String row;
            while ((row = csvReader.readLine()) != null) {
                count++;
                if (count == 1)
                    continue;
                String[] data = row.split(",");
                String mac = data[0];
                macPool.add(mac.hashCode());
            }
            csvReader.close();
        }catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void readWifi(){
        String csvFile = "/Users/yiminglin/Documents/Codebase/filter_optimization/data/smartbench/wifi.txt";
        String[] values;
        String delimiter = ",";
        try{
            File file = new File(csvFile);
            FileReader fr = new FileReader(file);
            BufferedReader br = new BufferedReader(fr);
            String line = "";
            int st=0, et=0, mac=0, lid=0, duration = 0;
            while((line = br.readLine()) != null) {
                values = line.split(delimiter);
                for (int i = 0; i < values.length; i++){
                    String val = values[i];
                    if(i==0){
                        st = Integer.valueOf(val);
                    }
                    if(i==1){
                        et = Integer.valueOf(val);
                    }
                    if(i==2){
                        mac = Integer.valueOf(val);
                    }
                    if(i==3){
                        lid = Integer.valueOf(val);
                    }
                    if(i==4){
                        duration = Integer.valueOf(val);
                    }
                }
                wifiTuples.add(new wifiTuple(st,et,mac,lid,duration));
            }
            br.close();
        }catch(IOException ioe) {
               ioe.printStackTrace();
        }
    }

    public void processWiFi(){
        String fileName = "/Users/yiminglin/Documents/Codebase/filter_optimization/data/smartbench/wifiClean.txt";
        File file = new File(fileName); 
        try {
            FileWriter out = new FileWriter(file);
            BufferedWriter bw=new BufferedWriter(out);

            for(int i=0;i<wifiTuples.size();i++){
                wifiTuple t = wifiTuples.get(i);
                if(t.getLid() == MISSING){
                    t.setLid(setLocation());
                }
                bw.write(t.toString());
                bw.newLine();
            }
            
            bw.flush();
            bw.close();
            }catch (IOException e) {e.printStackTrace();}
    }

    public void readOccupancy(){
        String csvFile = "/Users/yiminglin/Documents/Codebase/filter_optimization/data/smartbench/occupancy.txt";
        String[] values;
        String delimiter = ",";
        try{
            File file = new File(csvFile);
            FileReader fr = new FileReader(file);
            BufferedReader br = new BufferedReader(fr);
            String line = "";
            int lid=0, st=0, et=0, occupancy=0, type = 0;
            while((line = br.readLine()) != null) {
                values = line.split(delimiter);
                for (int i = 0; i < values.length; i++){
                    String val = values[i];
                    if(i==0){
                        lid = Integer.valueOf(val);
                    }
                    if(i==1){
                        st = Integer.valueOf(val);
                    }
                    if(i==2){
                        et = Integer.valueOf(val);
                    }
                    if(i==3){
                        occupancy = Integer.valueOf(val);
                    }
                    if(i==4){
                        type = Integer.valueOf(val);
                    }
                }
                occupancyTuples.add(new occupancyTuple(lid,st,et,occupancy,type));
            }
            br.close();
        }catch(IOException ioe) {
               ioe.printStackTrace();
        }
    }

    public void processOccupancy(){
        String fileName = "/Users/yiminglin/Documents/Codebase/filter_optimization/data/smartbench/occupancyClean.txt";
        File file = new File(fileName); 
        try {
            FileWriter out = new FileWriter(file);
            BufferedWriter bw=new BufferedWriter(out);

            for(int i=0;i<occupancyTuples.size();i++){
                occupancyTuple t = occupancyTuples.get(i);
                if(t.getType() == MISSING){
                    t.setType(setType());
                }
                if(t.getOccupancy() == MISSING){
                    t.setOccupancy(setOccupancy(t.getType()));
                }

                bw.write(t.toString());
                bw.newLine();
            }
            
            bw.flush();
            bw.close();
            }catch (IOException e) {e.printStackTrace();}
    }
    

    public static int nextGaussian(Random r, int mean, int deviation, int min, int max){
        double next = r.nextGaussian()*deviation+mean;
        if(next > max){
            next = max;
        }
        if(next < min){
            next = min;
        }
        return (int)next;
    }

    public void generateSpace(){
        String fileName = "/Users/yiminglin/Documents/Codebase/filter_optimization/data/smartbench/space.txt";
        File file = new File(fileName); 
        Random r = new Random();
        try {
            FileWriter out = new FileWriter(file);
            BufferedWriter bw=new BufferedWriter(out);

            
            //lid, building, floor, type, capacity
            for(int i=locationMIN; i<=locationMAX;i++){
                String line = "";
                //set lid
                line += i + ",";
                //set building: 30
                line += ThreadLocalRandom.current().nextInt(0, 30) + ",";
                //set floor: 6
                line += ThreadLocalRandom.current().nextInt(0, 6) + ",";
                //set type
                line += setType() + ",";
                //set capacity
                line += nextGaussian(r, 20, 5, 5, 100);
                bw.write(line);
                bw.newLine();
            }

            
            
            bw.flush();
            bw.close();
            }catch (IOException e) {e.printStackTrace();}

    }

    public void datagen(){
        //users
        // readusers();
        // processUsers();
        // //wifi
        // readWifi();
        // processWiFi();
        // //occupancy
        // readOccupancy();
        // processOccupancy();
        //space
        generateSpace();
    }

}
