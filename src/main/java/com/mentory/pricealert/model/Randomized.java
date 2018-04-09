package com.mentory.pricealert.model;

import java.util.Random;

/**
 * Created by anatolytikhonov on 27/09/2017.
 */
public abstract class Randomized {

    private final Random random = new Random();

    public <T extends Enum<?>> T randomEnum(Class<T> clazz){
        int x = random.nextInt(clazz.getEnumConstants().length);
        return clazz.getEnumConstants()[x];
    }
}
