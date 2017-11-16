package com.stockemotion.cfg.utils;

import com.stockemotion.common.utils.PropertiesGetter;

/**
 * Created by piguanghua on 2017/11/15.
 */
public class Environment {
   public static String  deployenv = PropertiesGetter.getValue("deployenv");
    public static String zkserver =  PropertiesGetter.getValue("zkserver");
}
