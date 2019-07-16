package big13.spark;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.gson.JsonObject;

import java.util.ArrayList;
import java.util.List;

public class TagUtils {

    public static List<String> parseJson(String json) {
        List<String> list = new ArrayList<String>();

        JSONObject jo1 = JSON.parseObject(json);

        JSONArray jarr = jo1.getJSONArray("extInfoList");

        if (jarr != null && jarr.size() > 0) {
            JSONObject firstObj = jarr.getJSONObject(0);
            JSONArray tagArr = firstObj.getJSONArray("values");

            if (tagArr != null && tagArr.size() > 0) {
                for (int i = 0; i < tagArr.size(); i++) {
                    String tag = tagArr.getString(i);
                    list.add(tag);
                }
            }

        }

        return list;

    }

}
