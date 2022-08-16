package com.atguigu.edu.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class DwsExaminationPaperScoreDurationExamWindowBean {

    // 窗口起始时间
    String stt;

    // 窗口结束时间
    String edt;

    // 试卷 ID
    String paper_id;

    // 试卷名称
    String paper_title;

    // 分数段
    String score_duration;

    // 用户数
    Long user_count;

    // 时间戳
    Long ts;
}
