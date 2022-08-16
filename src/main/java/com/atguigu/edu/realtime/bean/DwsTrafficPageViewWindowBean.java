package com.atguigu.edu.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class DwsTrafficPageViewWindowBean {

    // 窗口起始时间
    String stt;

    // 窗口结束时间
    String edt;

    // 设备 ID
    @TransientSink
    String mid;

    // 页面 ID
    @TransientSink
    String pageId;

    // 首页独立访客数
    Long homeUvCount;

    // 课程列表页独立访客数
    Long listUvCount;

    // 课程详情页独立访客数
    Long detailUvCount;

    // 时间戳
    Long ts;
}
