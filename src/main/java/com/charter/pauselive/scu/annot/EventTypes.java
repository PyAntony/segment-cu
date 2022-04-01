package com.charter.pauselive.scu.annot;

import javax.inject.Qualifier;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.*;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

public class EventTypes {
    @Qualifier
    @Target({METHOD, FIELD, PARAMETER, TYPE})
    @Retention(RUNTIME)
    public @interface SeekSuccess {
        boolean value();
    }
}
