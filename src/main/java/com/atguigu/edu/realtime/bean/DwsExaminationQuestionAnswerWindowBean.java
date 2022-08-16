package com.atguigu.edu.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class DwsExaminationQuestionAnswerWindowBean {

    // 窗口起始时间
    String stt;

    // 窗口结束时间
    String edt;

    // 题目 ID
    String question_id;

    // 题目内容
    String question_txt;

    // 正确答题次数
    Long correctAnswerCount;

    // 答题次数
    Long answer_count;

    // 时间戳
    Long ts;
}
