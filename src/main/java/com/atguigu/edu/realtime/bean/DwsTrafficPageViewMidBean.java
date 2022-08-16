package com.atguigu.edu.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@Deprecated
public class DwsTrafficPageViewMidBean {
    // 设备 ID
    String mid;

    // 页面 ID
    String pageId;

    // 时间戳
    Long ts;
}
