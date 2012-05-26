/***************************************************************************
 *  Copyright (C) 2012 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  Alex Kalinin (akalinin@cs.brown.edu)                                   *
 *  http://www.cs.brown.edu/~akalinin/                                     *
 *                                                                         *
 *  Permission is hereby granted, free of charge, to any person obtaining  *
 *  a copy of this software and associated documentation files (the        *
 *  "Software"), to deal in the Software without restriction, including    *
 *  without limitation the rights to use, copy, modify, merge, publish,    *
 *  distribute, sublicense, and/or sell copies of the Software, and to     *
 *  permit persons to whom the Software is furnished to do so, subject to  *
 *  the following conditions:                                              *
 *                                                                         *
 *  The above copyright notice and this permission notice shall be         *
 *  included in all copies or substantial portions of the Software.        *
 *                                                                         *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,        *
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF     *
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. *
 *  IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR      *
 *  OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,  *
 *  ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR  *
 *  OTHER DEALINGS IN THE SOFTWARE.                                        *
 ***************************************************************************/

package edu.brown.benchmark.tpce.util;

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.TimeZone;
import org.voltdb.types.*;

import edu.brown.benchmark.tpce.TPCEConstants;

public class EGenDate {
    /*
     * The idea here is to set up a pure Gregorian calendar without a time zone offset
     * to use it for generating day numbers properly. This will correspond with the
     * canonical EGen implementation.
     * 
     * However, it should be noted that since the TIMESTAMP is used for the corresponding SQL data type,
     * the interpretation of the generated value depends on current time zone and daylight saving settings since
     * the generated value is just a number of milliseconds from the Epoch.
     * 
     * For example, the generated '1990-12-13' will be interpreted as '1990-12-12, 19:00' in EDT (daylight saving)
     *              the generated '1953-09-26' will be interpreted as '1953-09-25, 20:00' in EDT (no daylight saving)
     */
    public static final GregorianCalendar cal = new GregorianCalendar(TimeZone.getTimeZone("GMT")); // no time zone
    private static final int cur_year;
    private static final int cur_month;
    private static final int cur_day;
    private static int cur_hour;
    private static int cur_minute;
    private static int cur_second;
    private static int cur_msecond;
    public static final double    NsPerSecondDivisor      = 1000000000.0;
    public static final int     NsPerSecond             = 1000000000;
    public static final double    MsPerSecondDivisor      = 1000.000;
    public static final int     MsPerSecond             = 1000;
    public static final int     SecondsPerMinute        = 60;
    public static final int     MinutesPerHour          = 60;
    public static final int     HoursPerDay             = 24;
    public static final int     HoursPerWorkDay         = 8;
    public static final int     DaysPerWorkWeek         = 5;
    public static final int     DaysPerWeek             = 7;

    public static final int     SecondsPerHour      = SecondsPerMinute * MinutesPerHour;
    public static final int     SecondsPerDay       = SecondsPerMinute * MinutesPerHour * HoursPerDay;
    public static final int     SecondsPerWorkDay   = SecondsPerMinute * MinutesPerHour * HoursPerWorkDay;
    public static final int     MsPerDay            = SecondsPerDay * MsPerSecond;
    public static final int     MsPerWorkDay        = SecondsPerWorkDay * MsPerSecond;
    
    static {
        cal.setGregorianChange(new Date(Long.MIN_VALUE)); // pure Gregorian calendar
        cur_year = cal.get(Calendar.YEAR);
        cur_month = cal.get(Calendar.MONTH);
        cur_day = cal.get(Calendar.DAY_OF_MONTH);
        cal.setTimeInMillis(0);
    }
    
    public static int getYear() {
        return cur_year;
    }
    
    public static int getMonth() {
        return cur_month;
    }
    
    public static int getDay() {
        return cur_day;
    }
    
    public static int getHour() {
    	cur_hour = cal.get(Calendar.HOUR_OF_DAY);
        return cur_hour;
    }
    
    public static int getMinute() {
    	cur_minute = cal.get(Calendar.MINUTE);
        return cur_minute;
    }
    
    public static int getSecond() {
    	cur_second = cal.get(Calendar.SECOND);
        return cur_second;
    }
    
    public static int getMSecond() {
    	cur_msecond = cal.get(Calendar.MILLISECOND);
        return cur_msecond;
    }
    
    public static int getCurrentDayNo() {
        return getDayNo(cur_year, cur_month, cur_day);
    }
    
    /*
     * Get day number from 01/01/0001 (1 AD)
     */
    public static int getDayNo(int year, int month, int day) {
        cal.set(year, month, day);
        long dateEpochMillis = cal.getTimeInMillis();
        
        // months start from 0 -- we need January here 
        cal.set(1, 0, 1);
        long startEpochMillis = cal.getTimeInMillis();
        
        long dateMillis = dateEpochMillis - startEpochMillis;
        assert(dateMillis >= 0);
        
        // return day number: 1sec = 1000msecs, 1hour = 3600secs, 1day = 24hours; 'int' should be enough
        return (int)(dateMillis / 1000 / 3600 / 24);
    }
    
    public static Date getDateFromDayNo(int dayNo) {
        // to msec based on 01/01/01: 1sec = 1000msecs, 1hour = 3600secs, 1day = 24hours; 'int' should be enough
        long dateMillis = (long)dayNo * 24 * 3600 * 1000;
        
        // months start from 0 -- we need January here
        cal.set(1, 0, 1);
        long startEpochMillis = cal.getTimeInMillis();
        
        // we need msecs from the Epoch to set up the date
        long dateEpochMillis = dateMillis + startEpochMillis;
        cal.setTimeInMillis(dateEpochMillis);
        
        return cal.getTime();
    }
    
    public static Date getDateFromYMD(int year, int month, int day) {
        cal.set(year, month, day);
        
        return cal.getTime();
    }
    //TODO not thread safe
    public static Date getDateFromTime(int year, int month, int day, int hour, int min, int sec, int msec) {
        cal.set(year, month, day, hour, min, sec);
        cal.set(Calendar.MILLISECOND, msec);
        
        return cal.getTime();
    }
    
    public static Date addDaysMsecs(Date time, int daysAdd, int msecsAdd, boolean adjustWeekend) {
        if (adjustWeekend) {
            daysAdd = ((daysAdd / TPCEConstants.daysPerWorkWeek) * 7) + (daysAdd % TPCEConstants.daysPerWorkWeek);
        }
        
        cal.setTime(time);
        cal.add(Calendar.DAY_OF_MONTH, daysAdd);
        cal.add(Calendar.MILLISECOND, msecsAdd);
        
        return cal.getTime();
    }
    
    public static Date AddMinutes(Date date, int minutes){
    	return (Date)addDaysMsecs( date, 0, minutes * SecondsPerMinute * MsPerSecond, false);
    }
    
    public static Date AddWorkMs(Date date, long WorkMs){
        int WorkDays  = (int)(WorkMs / (long) MsPerWorkDay);
        return (Date)addDaysMsecs( date, WorkDays, (int)(WorkMs % MsPerWorkDay), true );
    }
    
    public static TimestampType getTimeStamp(Date date){
    	return new TimestampType(date);
    }
}


