package org.vanilladb.core.filter;
import java.util.*;
import org.vanilladb.core.sql.*;
import org.vanilladb.core.query.algebra.*;

public class filterPlan{
    public static boolean enable = false;//to use new filters or not 
    public static HashMap<String, List<filter>> filters = new HashMap<>();//store attr to a set of filters that are applicable on it 
    public static HashMap<String, Boolean> attrs = new HashMap<>();

    public static int numberOfDroppedTuple = 0;

    public static void enable(){
        enable = true;
    }

    public static void disable(){
        enable = false;
    }

    public static void addFilter(String attr, String filterType, Constant low, Constant high, Boolean low_include, Boolean high_include, Boolean is_low, Boolean is_high){
        if(enable){
            filter f = new filter(attr, filterType, low, high, low_include, high_include, is_low, is_high);
            if(filters.containsKey(attr)){
                filters.get(attr).add(f);
            }else{
                List<filter> new_filters = new ArrayList<>();
                new_filters.add(f);
                filters.put(attr, new_filters);
            }
        }
    }

    public static void addFilter(String attr, String filterType, HashMap<Constant, Boolean> memberships){
        if(enable){
            filter f = new filter(attr, filterType, memberships);
            if(filters.containsKey(attr)){
                filters.get(attr).add(f);
            }else{
                List<filter> new_filters = new ArrayList<>();
                new_filters.add(f);
                filters.put(attr, new_filters);
            }
        }
    }

    public static boolean checkFilter(String attr, TableScan ts){//ts is a tuple 
        if(!enable){
            return true;
        }
        if(!attrs.containsKey(attr)){
            return true;
        }
        Constant value = ts.getVal(attr);
        for(int i=0;i<filters.get(attr).size();i++){
            filter f = filters.get(attr).get(i);
            if(f.filterType.equals("max")){
                if(value.compareTo(f.high) < 0){
                    return false;
                }
            }else if(f.filterType.equals("min")){
                if(value.compareTo(f.high) > 0){
                    return false;
                }
            }else if(f.filterType.equals("membership")){
                if(!f.memberships.containsKey(value)){
                    return false;
                }
            }
        }
        return true;
    }

    public static boolean checkFilter(Scan t){//t is a tuple
        if(!enable){
            return true;
        }
        for(String attr: attrs.keySet()){
            if(t.hasField(attr)){
                Constant value = t.getVal(attr);
                //check all filters that are applicable to attr
                for(int i=0;i<filters.get(attr).size();i++){
                    filter f = filters.get(attr).get(i);
                    if(f.filterType.equals("max")){
                        if(value.compareTo(f.high) < 0){
                            return false;
                        }
                    }else if(f.filterType.equals("min")){
                        if(value.compareTo(f.high) > 0){
                            return false;
                        }
                    }else if(f.filterType.equals("membership")){
                        if(!f.memberships.containsKey(value)){
                            return false;
                        }
                    }
                }
            }
        }
        return true;
    }

    /*
     * We do not update membership filter for now, onlt update the range filter
     */

    public static boolean updateFilter(String filterType, String attr, Constant low, Constant high, HashMap<Constant, Boolean> memberships){
        if(!enable){
            return false;
        }
        if(!filters.containsKey(attr)){
            return false;
        }
        for(int i=0;i<filters.get(attr).size();i++){
            filter f = filters.get(attr).get(i);
            if(f.filterType.equals(filterType)){
                if(filterType.equals("max")){//attr >= low
                    f.low = low;
                }else if(filterType.equals("min")){
                    f.high = high;
                }else if(filterType.equals("membership")){
                    f.memberships = memberships;
                }
                return true;
            }
        }
        return false;
    }

    public static void printFilter(){
        
    }
}