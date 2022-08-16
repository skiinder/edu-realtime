package com.atguigu.edu.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class DwsExaminationPaperExamWindowBean {

    // 窗口起始时间
    String stt;

    // 窗口结束时间
    String edt;

    // 试卷 ID
    String paperId;

    // 试卷名称
    String paperTitle;

    // 课程 ID
    String courseId;

    // 课程名称
    String courseName;

    // 考试人次
    Long examTakenCount;

    // 考试总分
    Long examTotalScore;

    // 考试总时长
    Long examTotalDuringSec;

    // 时间戳
    Long ts;
}
