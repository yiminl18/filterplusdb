package org.vanilladb.core.filter;

import java.util.*;

import org.vanilladb.core.sql.Constant;

public class filter{
    /*
     * This class implements new filters discovered at query runtime.
     * Equalrange is the range filter generated from equal join, and only generated in fast learning phase, will not be generated in query run phase. 
     */
    public String attr;
    public String filterType;//"max", "min", "membership", "range", "groupmax", "groupmin", "equalrange"
    //range filters
    public Constant low, high;
    public boolean low_include, high_include, is_low, is_high;//is low: low side valid or not, low include, low side include (>=) or not (>)
    //membership filters
    public HashMap<Constant, Boolean> memberships;
    private Constant groupVal; //for now, only group by one attr 
    private String groupField;

    //construct range filter
    public filter(String attr, String filterType, Constant groupVal, String groupField, Constant low, Constant high, Boolean low_include, Boolean high_include, Boolean is_low, Boolean is_high){
        this.attr = attr;
        this.filterType = filterType;
        this.low = low;
        this.high = high;
        this.low_include = low_include;
        this.high_include = high_include;
        this.is_low = is_low;
        this.is_high = is_high;
        this.groupVal = groupVal;
        this.groupField = groupField;
    }

    public String getGroupField(){
        return this.groupField;
    }

    public Constant getGroupVal(){
        return this.groupVal;
    }

    //construct membership filter 
    public filter(String attr, String filterType, HashMap<Constant, Boolean> memberships){
        this.attr = attr;
        this.filterType = filterType;
        this.memberships = memberships;
    }

    /*
     * Change range filter to string form
     */
    public String toString(){
        String predicate = attr;
        if(filterType.equals("range") || filterType.equals("equalrange")){
            if(is_high){//< or <=
                if(high_include){//<=
                    predicate += "<=";
                }else{//< 
                    predicate += "<";
                }
                predicate += high.toString();
            }
            if(is_low){// > or >= 
                String p2 = attr; 
                if(low_include){//>=
                    p2 += ">=";
                }else{
                    p2 += ">";
                }
                if(is_high){
                    p2 += low.toString();
                    predicate += " and " + p2;
                }else{
                    predicate = p2;
                }
                
            }
        }

        System.out.println("in filter " + predicate);
        return predicate;
    }

    public void print(){
        System.out.println(filterType + " " + attr + " " + low + " " + high);
    }
}

