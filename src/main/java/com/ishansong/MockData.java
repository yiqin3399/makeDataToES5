package com.ishansong;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.ishansong.common.ElasticSearchUtil;
import com.ishansong.model.CourierGrowthBean;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.UnknownHostException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

/**
 * @author mr
 * @date 2019/08/27
 */
public class MockData {

    private static TransportClient transportClient;
    private static BulkRequestBuilder bulkRequest;
    private static String uuid;
    static FileWriter fw;


    static{
        String hostPort = "bigdata-dev-es-1:9300,bigdata-dev-es-2:9300,bigdata-dev-es-3:9300,bigdata-dev-es-4:9300";
        try {
            transportClient = ElasticSearchUtil.getTransportClient(hostPort);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

        try {
            fw = new FileWriter("/Users/yiqin/Desktop/courierGrowthIdtt.txt",true);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static String getCourierGrowthBean(){
        Random random = new Random();
        CourierGrowthBean courierGrowthBean = new CourierGrowthBean();
        courierGrowthBean.setBadReviewCount(random.nextInt(100));
        courierGrowthBean.setCalTime(random.nextInt(20000000)+1570723200000l);
        courierGrowthBean.setCityId(1101);
        courierGrowthBean.setCompetitionId(26);
        long courierId = getCourierId();
        uuid = String.valueOf(courierId);
        writeToTxt(uuid);
        courierGrowthBean.setCourierId(courierId);
        courierGrowthBean.setDeliveriesCount(random.nextInt(1000)+1);
        courierGrowthBean.setGrade(random.nextInt(8)+1);
        courierGrowthBean.setProtect(true);
        courierGrowthBean.setProtectGrade(6);
        courierGrowthBean.setRefuseCount(random.nextInt(100));
        courierGrowthBean.setGrowValue(random.nextInt(90000));
        courierGrowthBean.setWorkDayCount(random.nextInt(200));
        return JSON.toJSONString(courierGrowthBean, SerializerFeature.WriteMapNullValue);

    }

    /**
     * 生成唯一的闪送员id
     */
    public static Long getCourierId() {
        SimpleDateFormat df = new SimpleDateFormat("HHmmssSSS");
        String date = df.format(new Date());

        NumberFormat f = new DecimalFormat("00000");
        Random r = new Random();
        String number = f.format(r.nextInt(10000));

        Long orderNumber = Long.parseLong(date + number);
        return orderNumber;
    }


    public static void main(String[] args) {
        String index = "courier_growth_system_result";
        String type = "courier_growth_system_result";

        bulkRequest = transportClient.prepareBulk();

        long a = System.currentTimeMillis(); //开始时间

        int num = 0;
        for(int i=0; i< 20000;i++) {//总共往ES里放的数据量，2万条

            bulkRequest.add(transportClient.prepareIndex(index, type)
                    .setSource(getCourierGrowthBean(), XContentType.JSON)
                    .setId(uuid));
            num ++;
            if (num >= 1000) {//1次1000条
                if (bulkRequest.numberOfActions() > 0) {
//                    bulkRequest.get();
                    bulkRequest.execute().actionGet();
                }
                num = 0;
                bulkRequest = transportClient.prepareBulk();
            }

        }

        if (bulkRequest.numberOfActions() > 0) {
//            bulkRequest.get();
            bulkRequest.execute().actionGet();
        }


        long b = System.currentTimeMillis();

        System.out.println(b-a);

        if(fw!=null){
            try {
                fw.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    static int numTemp =0;//一行写十个参数
    public static void writeToTxt(String courierId){
        PrintWriter pw = new PrintWriter(fw);
        if(numTemp<10){
            pw.print(courierId);
            pw.print(",");
            numTemp++;
        }else if(numTemp==10){
            numTemp=0;
            pw.println();
        }
        pw.flush();
    }

}
