package org.vanilladb.core.filter;

import java.util.HashMap;

import org.vanilladb.core.sql.Constant;

public class filter{
    /*
     * This class implements new filters discovered at query runtime
     */
    public String attr;
    public String filterType;//"max", "min" or "membership"
    //range filters
    public Constant low, high;
    public boolean low_include, high_include, is_low, is_high;//is low: low side valid or not, low include, low side include (>=) or not (>)
    //membership filters
    public HashMap<Constant, Boolean> memberships;

    //construct range filter
    public filter(String attr, String filterType, Constant low, Constant high, Boolean low_include, Boolean high_include, Boolean is_low, Boolean is_high){
        this.attr = attr;
        this.filterType = filterType;
        this.low = low;
        this.high = high;
        this.low_include = low_include;
        this.high_include = high_include;
        this.is_low = is_low;
        this.is_high = is_high;
    }

    //construct membership filter 
    public filter(String attr, String filterType, HashMap<Constant, Boolean> memberships){
        this.attr = attr;
        this.filterType = filterType;
        this.memberships = memberships;
    }

    public void print(){
        System.out.println(filterType + " " + attr + " " + low + " " + high);
    }
}

