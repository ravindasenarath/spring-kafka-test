package com.ravinda.kafka;

import org.junit.jupiter.api.Test;

public class MySimpleTest extends AbstractSpringBootTest {

    @Test
    public void myTest()throws Exception{
        publishTick("test");
        Thread.sleep(10000);
    }

}
