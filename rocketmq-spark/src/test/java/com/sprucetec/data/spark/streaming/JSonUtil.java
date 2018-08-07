package com.sprucetec.data.spark.streaming;

import com.alibaba.fastjson.JSONObject;

/**
 * 公司：美菜网
 * 部门：数据平台开发部
 * 作者：韩家富
 * 邮箱:hanjiafu@meicai.cn
 * 描述：
 */
public class JSonUtil {
    public static JSONObject merge(JSONObject arg1, JSONObject arg2){
        JSONObject conf = new JSONObject();
        conf.putAll(arg1);
        conf.putAll(arg2);
        return conf;
    }
}
