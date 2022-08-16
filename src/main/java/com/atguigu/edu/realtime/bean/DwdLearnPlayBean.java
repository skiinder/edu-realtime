package com.atguigu.edu.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class DwdLearnPlayBean {
    // 来源 ID，对应于 common.sc
    String sourceId;

    // 省份 ID，对应于 common.ar
    String provinceId;

    // 用户 ID，对应于 common.uid
    String userId;

    // 操作系统，对应于 common.os
    String operatingSystem;

    // 渠道，对应于 common.ch
    String channel;

    // 新老访客状态标记，1 为 true 0 为 false，对应于 common.is_new
    String isNew;

    // 设备型号，对应于 common.md
    String model;

    // 设备 ID，对应于 common.mid
    String machineId;

    // 版本号，对应于 common.vc
    String versionCode;

    // 品牌，对应于 common.ba
    String brand;

    // 会话 ID，对应于 common.sid
    String sessionId;

    // 播放时长，对应于 appVideo.play_sec
    Integer playSec;

    // 播放时长，对应于 appVideo.position_sec
    Integer positionSec;

    // 视频 ID，对应于 appVideo.video_id
    String videoId;

    // 时间戳，对应于 ts
    Long ts;
}
