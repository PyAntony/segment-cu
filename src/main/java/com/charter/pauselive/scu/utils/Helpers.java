package com.charter.pauselive.scu.utils;

import io.vavr.Function1;

public class Helpers {

    /***
     * Calculate the initial time of the bucket where {@code currTimestamp} sits with respect to
     * a timestamp {@code anchor}. Function expects timestamps in seconds (10 digits). Example:
     * <br><br>
     * anchor = 2022-11-13T00:00:00Z<br>
     * currTimestamp = 2022-11-13T00:00:13Z<br>
     * windowSec = 5<br>
     *
     * result = 2022-11-13T00:00:10Z
     *
     * @param anchor starting timestamp.
     * @param windowSec length of buckets in seconds
     * @param currTimestamp the referred timestamp.
     * @param fmt a String formatter to map the timestamp.
     */
    public static String startTimeFromAnchor(
        long anchor, int windowSec, Function1<Long, String> fmt, long currTimestamp
    ) {
        return fmt.apply(anchor + ((currTimestamp - anchor) / windowSec) * windowSec);
    }
}
