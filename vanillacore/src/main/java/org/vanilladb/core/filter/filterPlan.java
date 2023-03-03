package org.vanilladb.core.filter;
import java.util.*;
import org.vanilladb.core.sql.*;
import org.vanilladb.core.query.algebra.*;
import org.vanilladb.core.query.planner.JoinKnob;

public class filterPlan{
    public static boolean enableMaxmin = false, enableEqual = false, enableTheta = false, enableGroup = false;
    public static HashMap<String, List<filter>> filters = new HashMap<>();//store attr to a set of filters that are applicable on it
    public static HashMap<Constant, List<filter>> groupFilters = new HashMap<>(); //store a set of group filters, one group val could have multiple range filters  
    public static String groupFld;//groupFld is the attribute that group by is applied on
    public static List<String> groupPredAttr = new ArrayList<>();//groupPredAttr is the attribute that the group filter (max or in) is applied on 
    //public static List<S>

    public static int numberOfDroppedTuplefromMAXMIN = 0;
    public static int numberOfDroppedTuplefromEqual = 0;
    public static int numberOfDroppedTuplefromTheta = 0;
    public static int numberOfDroppedTuplefromGroup = 0;

    public static void init(){
        filters = new HashMap<>();
        groupFilters = new HashMap<>();
        groupPredAttr = new ArrayList<>();
        close();
    }

    public static void enableMaxminFilter(){
        enableMaxmin = true;
    }

    public static void enableEqualJoinFilter(){
        enableEqual = true;
    }

    public static void enableThetaJoinFilter(){
        enableTheta = true;
    }

    public static void enableGroupFilter(){
        enableGroup = true;
    }

    public static void open(){//open all filters
        enableMaxmin = true;
        enableEqual = true;
        enableTheta = true;
        enableGroup = true;
    }

    public static void close(){//close all filters 
        enableMaxmin = false;
        enableEqual = false;
        enableTheta = false;
        enableGroup = false;
    }

    public static void addFilter(String attr, String filterType, Constant groupVal, String groupField, Constant low, Constant high, Boolean low_include, Boolean high_include, Boolean is_low, Boolean is_high){
        
        filter f = new filter(attr, filterType, groupVal, groupField, low, high, low_include, high_include, is_low, is_high);
        //add group filter into groupFilters instead of filters, since the number of group filters might be large, and adding to filters will have efficiency problem 

        
        if(filterType.equals("groupmax") || filterType.equals("groupmin")){
            if(!JoinKnob.fastLearning){
                groupFld = groupField;
                if(!groupPredAttr.contains(attr)){
                    groupPredAttr.add(attr);
                }
                List<filter> new_filters = new ArrayList<>();
                new_filters.add(f);
                if(groupVal != null && !groupFilters.containsKey(groupVal)){
                    groupFilters.put(groupVal, new_filters);
                }
            }
        }else{
            if(JoinKnob.rawRun){
                if(filters.containsKey(attr)){
                    filters.get(attr).add(f);
                }else{
                    List<filter> new_filters = new ArrayList<>();
                    new_filters.add(f);
                    filters.put(attr, new_filters);
                }
            }else{
                if(JoinKnob.fastLearning){//direcly use already learned filter, membership filter and theta join are optimal
                    if(filters.containsKey(attr)){
                        filters.get(attr).add(f);
                    }else{
                        List<filter> new_filters = new ArrayList<>();
                        new_filters.add(f);
                        filters.put(attr, new_filters);
                    }
                }
            }
        }
    }

    public static void addFilter(String attr, String filterType, HashMap<Constant, Boolean> memberships){
        filter f = new filter(attr, filterType, memberships);
        if(filters.containsKey(attr)){
            filters.get(attr).add(f);
        }else{
            List<filter> new_filters = new ArrayList<>();
            new_filters.add(f);
            filters.put(attr, new_filters);
        }
    }

    public static boolean checkFilter(String attr, TableScan ts){//ts is a tuple 
        if(!filters.containsKey(attr)){
            return true;
        }
        Constant value = ts.getVal(attr);
        for(int i=0;i<filters.get(attr).size();i++){//scan all filters corresponding to attr
            filter f = filters.get(attr).get(i);
            if(f.filterType.equals("max")){
                if(!enableMaxmin){//max,min filter is closed, do not check 
                    continue;
                }
                if(f.low == null){
                    continue;
                }
                if(value.compareTo(f.low) < 0){
                    numberOfDroppedTuplefromMAXMIN ++;
                    return false;
                }
            }else if(f.filterType.equals("min")){
                if(!enableMaxmin){//max,min filter is closed, do not check 
                    continue;
                }
                if(f.high == null){
                    continue;
                }
                if(value.compareTo(f.high) > 0){
                    numberOfDroppedTuplefromMAXMIN ++;
                    return false;
                }
            }else if(f.filterType.equals("membership")){
                if(!enableEqual){//equal filter is closed, do not check 
                    continue;
                }
                if(!f.memberships.containsKey(value)){
                    numberOfDroppedTuplefromEqual ++;
                    return false;
                }
            }
            else if(f.filterType.equals("range")){
                if(!enableTheta){
                    continue;
                }
                if(f.is_low){//low<value
                    if(f.low_include && f.low != null && value.compareTo(f.low) < 0){//filter is low <= value, but the fact is low > value
                        numberOfDroppedTuplefromTheta ++;
                        return false;
                    }
                    if(!f.low_include && f.low != null && value.compareTo(f.low) <= 0){//filter is low < value, but the fact is low >= value
                        numberOfDroppedTuplefromTheta ++;
                        return false;
                    }
                }
                if(f.is_high){//value<high
                    if(f.high_include && f.high != null && value.compareTo(f.high) > 0){//filter is value <= high, but the fact is value > high
                        numberOfDroppedTuplefromTheta ++;
                        return false;
                    }
                    if(!f.high_include && f.high != null && value.compareTo(f.high) >= 0){//filter is value < high, but the fact is value >= high
                        numberOfDroppedTuplefromTheta ++;
                        return false;
                    }
                }
            }
        }
        //check for group filter 
        if(!enableGroup){//group filter is closed, do not check 
            return true;
        }
        if(!ts.hasField(groupFld)){//group filter is not applicable 
            return true;
        }
        Constant gVal = ts.getVal(groupFld);
        if(!groupFilters.containsKey(gVal)){//group filter is not applicable 
            return true;
        }
        for(int i=0;i<groupFilters.get(gVal).size();i++){
            filter f = groupFilters.get(gVal).get(i);
            if(f.filterType.equals("groupmax")){
                if(f.low == null){
                    continue;
                }
                //conditional range filter 
                if(ts.getVal(f.getGroupField()).compareTo(f.getGroupVal()) == 0 && value.compareTo(f.low) < 0){
                    numberOfDroppedTuplefromGroup ++;
                    return false;
                }
            }else if(f.filterType.equals("groupmin")){
                if(f.high == null){
                    continue;
                }
                if(ts.getVal(f.getGroupField()).compareTo(f.getGroupVal()) == 0 && value.compareTo(f.high) > 0){
                    numberOfDroppedTuplefromGroup ++;
                    return false;
                }
            }
        }
        return true;
    }

    public static boolean checkFilter(Scan ts){//t is a tuple, true means passing the check
        for(String attr: filters.keySet()){
            if(ts.hasField(attr)){
                Constant value = ts.getVal(attr);
                //check all filters that are applicable to attr
                for(int i=0;i<filters.get(attr).size();i++){
                    filter f = filters.get(attr).get(i);
                    if(f.filterType.equals("max")){
                        if(!enableMaxmin){//max,min filter is closed, do not check 
                            continue;
                        }
                        if(f.low == null){
                            continue;
                        }
                        if(value.compareTo(f.low) < 0){
                            numberOfDroppedTuplefromMAXMIN ++;
                            return false;
                        }
                    }else if(f.filterType.equals("min")){
                        if(!enableMaxmin){//max,min filter is closed, do not check 
                            continue;
                        }
                        if(f.high == null){
                            continue;
                        }
                        if(value.compareTo(f.high) > 0){
                            numberOfDroppedTuplefromMAXMIN ++;
                            return false;
                        }
                    }else if(f.filterType.equals("membership")){
                        if(!enableEqual){//equal filter is closed, do not check 
                            continue;
                        }
                        if(!f.memberships.containsKey(value)){
                            numberOfDroppedTuplefromEqual ++;
                            return false;
                        }
                    }else if(f.filterType.equals("range")){
                        if(!enableTheta){
                            continue;
                        }
                        if(f.is_low){//low<value
                            if(f.low_include && f.low != null && value.compareTo(f.low) < 0){//filter is low <= value, but the fact is low > value
                                numberOfDroppedTuplefromTheta ++;
                                return false;
                            }
                            if(!f.low_include && f.low != null && value.compareTo(f.low) <= 0){//filter is low < value, but the fact is low >= value
                                numberOfDroppedTuplefromTheta ++;
                                return false;
                            }
                        }
                        if(f.is_high){//value<high
                            if(f.high_include && f.high != null && value.compareTo(f.high) > 0){//filter is value <= high, but the fact is value > high
                                numberOfDroppedTuplefromTheta ++;
                                return false;
                            }
                            if(!f.high_include && f.high != null && value.compareTo(f.high) >= 0){//filter is value < high, but the fact is value >= high
                                numberOfDroppedTuplefromTheta ++;
                                return false;
                            }
                        }
                    }
                }
            }
        }
        //check for group filter 
        if(!enableGroup){
            return true;
        }
        if(groupFld == null){
            return true;
        }
        if(!ts.hasField(groupFld)){//group filter is not applicable 
            return true;
        }
        Constant gVal = ts.getVal(groupFld);
        if(!groupFilters.containsKey(gVal)){//group filter is not applicable 
            return true;
        }
        for(int j=0;j<groupPredAttr.size();j++){//scan each group pred attr
            String attr = groupPredAttr.get(j);//attr is the attribute that max or min is applied on 
            if(ts.hasField(attr)){//if current tuple has some attr
                Constant value = ts.getVal(attr);
                for(int i=0;i<groupFilters.get(gVal).size();i++){
                    filter f = groupFilters.get(gVal).get(i);
                    if(!f.attr.equals(attr)){//if current attr is inconsistent with attr to search, stop 
                        continue;
                    }
                    if(f.filterType.equals("groupmax")){
                        if(f.low == null){
                            continue;
                        }
                        if(ts.getVal(f.getGroupField()).compareTo(f.getGroupVal()) == 0 && value.compareTo(f.low) < 0){
                            numberOfDroppedTuplefromGroup ++;
                            return false;
                        }
                    }else if(f.filterType.equals("groupmin")){
                        if(f.high == null){
                            continue;
                        }
                        if(ts.getVal(f.getGroupField()).compareTo(f.getGroupVal()) == 0 && value.compareTo(f.high) > 0){
                            numberOfDroppedTuplefromGroup ++;
                            return false;
                        }
                    }
                }
            }
        }
        
        return true;
    }

    /*
     * We do not update membership filter for now, only update the range and group filter.
     */

    public static boolean isGroupFilterCreated(Constant gVal){
        return groupFilters.containsKey(gVal);
    }

    public static boolean updateFilter(String filterType, String attr, Constant gVal, Constant low, Constant high){
        if(!filters.containsKey(attr)){
            return false;
        }
        for(int i=0;i<filters.get(attr).size();i++){//for each filter corresponding to attr 
            filter f = filters.get(attr).get(i);
            if(f.filterType.equals(filterType)){
                if(filterType.equals("max")){//attr >= low
                    f.low = low;
                }else if(filterType.equals("min")){
                    f.high = high;
                }
            }
        }
        if(groupFilters.containsKey(gVal)){
            for(int i=0;i<groupFilters.get(gVal).size();i++){//scan each group filter corresponding to gVal
                filter f = groupFilters.get(gVal).get(i);
                if(filterType.equals("groupmax")){//attr >= low
                    f.low = low;
                }else if(filterType.equals("groupmin")){
                    f.high = high;
                }
            }
        }
        return true;
    }
    /*
     * Merge learned filters and add them to current query 
     */
    public static String mergePredicate(String query){
        String newQ = "";
        String postQ = " ";
        if(!query.contains("groupby")){
            newQ = query;
        }else{
            int p = query.indexOf("groupby");
            newQ = query.substring(0, p);
            postQ = query.substring(p, query.length()-1);
        }
        
        for(Map.Entry<String, List<filter>> entry : filters.entrySet()){
            for(filter f: entry.getValue()){
                if(f.filterType.equals("range")){
                    newQ += " and ";
                    newQ += f.toString();
                }
            }
        }
        return newQ+postQ;
    }


    public static void printFilter(){
        //HashMap<String, List<filter>> filters = new HashMap<>();
        for(Map.Entry<String, List<filter>> entry : filters.entrySet()){
            System.out.println(entry.getKey());
            List<filter> filter = entry.getValue();
            for(int i=0;i<filter.size();i++){
                filter.get(i).print();
            }
        }
        int numOfGroupFilter = groupFilters.size()*groupPredAttr.size();
        System.out.println("Number of group filters is: " + numOfGroupFilter);
        if(numOfGroupFilter > 100){
            return ;
        }
        for(Map.Entry<Constant, List<filter>> entry : groupFilters.entrySet()){
            System.out.println(entry.getKey());
            List<filter> filter = entry.getValue();
            for(int i=0;i<filter.size();i++){
                filter.get(i).print();
            }
        }
    }

    public static void filterStats(){
        System.out.println("number of saved tuples from max/min, equal, theta join, group filter: " + numberOfDroppedTuplefromMAXMIN + " " + numberOfDroppedTuplefromEqual + " " + numberOfDroppedTuplefromTheta + " " + numberOfDroppedTuplefromGroup);
    }
}