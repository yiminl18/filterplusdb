package org.vanilladb.core.filter;

import java.util.HashMap;

import org.vanilladb.core.sql.Constant;

public class filter{
    /*
     * This class implements new filters discovered at query runtime
     */
    public String attr;
    public String filterType;//"range", or "membership"
    //range filters
    public double low, high;
    public boolean low_include, high_include;
    //membership filters
    public HashMap<Constant, Boolean> memberships;

    //construct range filter
    public filter(String attr, String filterType, Double low, Double high, Boolean low_include, Boolean high_include){
        this.attr = attr;
        this.filterType = filterType;
        this.low = low;
        this.high = high;
        this.low_include = low_include;
        this.high_include = high_include;
    }

    //construct membership filter 
    public filter(String attr, String filterType, HashMap<Constant, Boolean> memberships){
        this.attr = attr;
        this.filterType = filterType;
        this.memberships = memberships;
    }
}

