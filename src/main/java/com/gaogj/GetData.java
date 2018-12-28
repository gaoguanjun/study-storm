package com.gaogj;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Random;

/**
 * Created by Administrator on 2018/12/22.
 */
public class GetData {

    public static void main(String []args){
        File mySpoutFile = new File ("mySpoutFile");
        Random random = new Random();
        String hosts = "www.taobao.com";
        String []sessionIds = {"AAAAAAAAAAAAA","BBBBBBBBBBBB","CCCCCCCCCCCCCCCCC","DDDDDDDDDDDDD","EEEEEEE"};
        StringBuffer sb = new StringBuffer();
        for(int i = 0;i<1000;i++){
            sb.append(i+"\t"+hosts+"\t" + sessionIds[random.nextInt(5)]+"\t"+random.nextInt(50)+"\n");
        }
        if(!mySpoutFile.exists()){
            try {
                mySpoutFile.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        byte[] bytesArray = sb.toString().getBytes();
        FileOutputStream fo;
        try {
            fo = new FileOutputStream(mySpoutFile);
            fo.write(bytesArray);
            fo.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
