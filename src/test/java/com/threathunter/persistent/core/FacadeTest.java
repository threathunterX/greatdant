package com.threathunter.persistent.core;

import com.threathunter.model.Event;
import com.threathunter.persistent.core.api.LogsCreateContext;
import junit.framework.Assert;

import java.io.File;

/**
 * Created by yy on 17-10-17.
 */
public class FacadeTest {
    public void testLogsCreateContext(){
        LogsCreateContext context=new LogsCreateContext();
        context.startCreate();
        context.addLog(getEvent());
        context.stopCreate();
        //create file and dir is exists;
        File file=new File("");
        Assert.assertTrue(file.exists());
//        Assert.assertTu
    }

    public void testLogsReadContext(){

    }

    public Event getEvent(){
        return null;
    }

}
