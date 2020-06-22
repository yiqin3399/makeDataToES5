package com.ishansong.model;


import com.alibaba.fastjson.annotation.JSONField;
import lombok.Data;

@Data
public class CourierGrowthBean {
    long badReviewCount;
    long calTime;
    long cityId;
    long competitionId;
    long courierId;
    long deliveriesCount;
    long grade;
    long growValue;
    @JSONField(name="isProtect")
    boolean protect;
    long protectGrade;
    long refuseCount;
    long rewardGrowValue;
    long workDayCount;
}
