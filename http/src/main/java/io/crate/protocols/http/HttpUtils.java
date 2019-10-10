package io.crate.protocols.http;

import com.google.common.base.Splitter;
import com.google.common.collect.Maps;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * @author haipop Date: 2019/10/10 Time: 4:33 下午
 */
public class HttpUtils {

    private static final Splitter EQUAL = Splitter.on("=").omitEmptyStrings().trimResults();

    private static final Splitter SEMICOLON = Splitter.on(";").omitEmptyStrings().trimResults();

    public static Map<String, String> buildCookies(String cookie) {
        if (StringUtils.isEmpty(cookie)) {
            return Collections.emptyMap();
        }
        List<String> cookieItemList = SEMICOLON.splitToList(cookie);
        if (CollectionUtils.isEmpty(cookieItemList)) {
            return Collections.emptyMap();
        }
        Map<String, String> result = Maps.newHashMap();
        for (String cookieItem : cookieItemList) {
            List<String> dataList = EQUAL.splitToList(cookieItem);
            if (CollectionUtils.isEmpty(dataList) || dataList.size() != 2) {
                continue;
            }
            result.put(dataList.get(NumberUtils.INTEGER_ZERO), dataList.get(NumberUtils.INTEGER_ONE));
        }
        return result;
    }

}
