package com.atguigu.edu.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * description:
 * Created by 铁盾 on 2022/6/15
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class DwdUserUserLoginBean {
    // 用户 ID，对应于 common.uid
    String userId;

    // 登陆日期
    String dateId;

    // 登陆时间
    String loginTime;

    // 来源
    String sourceId;

    // 渠道，对应于 common.ch
    String channel;

    // 省份 ID，对应于 common.ar
    String provinceId;

    // 版本号，对应于 common.vc
    String versionCode;

    // 设备 ID，对应于 common.mid
    String midId;

    // 品牌，对应于 common.ba
    String brand;

    // 设备型号，对应于 common.md
    String model;

    // 操作系统，对应于 common.os
    String operatingSystem;

    // 时间戳
    Long ts;
}
