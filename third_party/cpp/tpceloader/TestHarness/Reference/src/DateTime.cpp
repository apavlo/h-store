/*
 * Legal Notice
 *
 * This document and associated source code (the "Work") is a preliminary
 * version of a benchmark specification being developed by the TPC. The
 * Work is being made available to the public for review and comment only.
 * The TPC reserves all right, title, and interest to the Work as provided
 * under U.S. and international laws, including without limitation all patent
 * and trademark rights therein.
 *
 * No Warranty
 *
 * 1.1 TO THE MAXIMUM EXTENT PERMITTED BY APPLICABLE LAW, THE INFORMATION
 *     CONTAINED HEREIN IS PROVIDED "AS IS" AND WITH ALL FAULTS, AND THE
 *     AUTHORS AND DEVELOPERS OF THE WORK HEREBY DISCLAIM ALL OTHER
 *     WARRANTIES AND CONDITIONS, EITHER EXPRESS, IMPLIED OR STATUTORY,
 *     INCLUDING, BUT NOT LIMITED TO, ANY (IF ANY) IMPLIED WARRANTIES,
 *     DUTIES OR CONDITIONS OF MERCHANTABILITY, OF FITNESS FOR A PARTICULAR
 *     PURPOSE, OF ACCURACY OR COMPLETENESS OF RESPONSES, OF RESULTS, OF
 *     WORKMANLIKE EFFORT, OF LACK OF VIRUSES, AND OF LACK OF NEGLIGENCE.
 *     ALSO, THERE IS NO WARRANTY OR CONDITION OF TITLE, QUIET ENJOYMENT,
 *     QUIET POSSESSION, CORRESPONDENCE TO DESCRIPTION OR NON-INFRINGEMENT
 *     WITH REGARD TO THE WORK.
 * 1.2 IN NO EVENT WILL ANY AUTHOR OR DEVELOPER OF THE WORK BE LIABLE TO
 *     ANY OTHER PARTY FOR ANY DAMAGES, INCLUDING BUT NOT LIMITED TO THE
 *     COST OF PROCURING SUBSTITUTE GOODS OR SERVICES, LOST PROFITS, LOSS
 *     OF USE, LOSS OF DATA, OR ANY INCIDENTAL, CONSEQUENTIAL, DIRECT,
 *     INDIRECT, OR SPECIAL DAMAGES WHETHER UNDER CONTRACT, TORT, WARRANTY,
 *     OR OTHERWISE, ARISING IN ANY WAY OUT OF THIS OR ANY OTHER AGREEMENT
 *     RELATING TO THE WORK, WHETHER OR NOT SUCH AUTHOR OR DEVELOPER HAD
 *     ADVANCE NOTICE OF THE POSSIBILITY OF SUCH DAMAGES.
 *
 * Contributors
 * - Charles Levine
 */

#include "../inc/EGenUtilities_stdafx.h"

using namespace TPCE;

// month array contains days of months for months in a non leap-year
static const INT32 monthArray[12] = { 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };

// month array contains days of months for months in a leap-year
static const INT32 monthArrayLY[12] = { 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };

// Checks whether the date/time is valid.
bool CDateTime::IsValid( INT32 year, INT32 month, INT32 day, INT32 hour, INT32 minute, INT32 second, INT32 msec )
{
    if (hour < 0 || hour > 23 || minute < 0 || minute > 59 ||
        second < 0 || second > 59 || msec < 0 || msec > 999)
        return false;

    if (year < 1 || year > 9999 || month < 1 || month > 12 || day < 1 || day > 31)
        return false;

    if (month != 2)  // if not February, then don't have to worry about leap years
    {
        return (day <= monthArray[month-1] );
    }
    else
    {
        INT32 FebDays = monthArray[1];
        // adjust days in February if this is a leap year
        if ((year % 4) == 0)        // only years which are multiples of 4 can be leap years
        {
            if ((year % 400) == 0)          // years divisible by 400 are leap years
            {
                FebDays++;
            }
            else
            {
                if ((year % 100) != 0)      // years divisible by 100 but not 400 are not leap years
                {
                    FebDays++;
                }
            }
        }
        return (day <= FebDays);
    }
}

// YMDtoDayno computes the number of days since Jan 1, 0001.  (Year 1 AD)
// 1-Jan-0001 = 0
INT32 CDateTime::YMDtoDayno( INT32 yr, INT32 mm, INT32 dd )
{
    assert( mm >= 1 && mm <= 12 );
    assert( dd >= 1 && dd <= 31 );
    assert( yr > 0 );

    // MonthArray contains cumulative days for months in a non leap-year
    static const INT32 MonthArray[] = { 0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334 };

    // days in 1-year period (not including any leap year exceptions) = 365
    static const INT32 dy1 = 365;
    // days in 4-year period (not including 400 and 100-year exceptions) = 1,461
    static const INT32 dy4 = 4 * dy1 + 1;           // fourth year is a leap year
    // days in 100-year period (not including 400-year exception) = 36,524
    static const INT32 dy100 = 25 * dy4 - 1;        // 100th year is not a leap year
    // days in 400-year period = 146,097
    static const INT32 dy400 = 4 * dy100 + 1;       // 400th year is a leap year

    // compute day of year
    INT32 jd = MonthArray[mm-1] + dd - 1;

    // adjust day of year if this is a leap year and it is after February
    if ((mm > 2) && (yr % 4)==0)    // only years which are multiples of 4 can be leap years
    {
        if ((yr % 400)==0)          // years divisible by 400 are leap years
            jd++;
        else if ((yr % 100)!=0)     // years divisible by 100 but not 400 are not leap years
            jd++;
    }

    // compute number of days from 1/1/0001 to beginning of present year
    yr--;   // start counting from year 1 AD (1-based instead of 0-based)
    jd += yr / 400 * dy400;
    yr %= 400;
    jd += yr / 100 * dy100;
    yr %= 100;
    jd += yr / 4 * dy4;
    yr %= 4;
    jd += yr * dy1;

    return jd;
}


// returns text representation of DateTime
// the style argument is interpreted as a two digit field, where the first digit (in the tens place)
// is the date format and the second digit (in the ones place) is the time format.
//
// The following formats are provided:
//      STYLE   DATE            TIME
//      -----   ----            ----
//      0       <omit>          <omit>
//      1       YYYY-MM-DD      HH:MM:SS        (24hr)
//      2       MM/DD/YY        HH:MM:SS.mmm    (24hr)
//      3       MM/DD/YYYY      HH:MM           (24hr)
//      4       DD-MON-YYYY     HH:MM:SS [AM|PM]
//      5       DD-MON-YY       HH:MM:SS.mmm [AM|PM]
//      6       MM-DD-YY        HH:MM [AM|PM]
//      7       MON DD YYYY
//      8       Month DD, YYYY
//
char* CDateTime::ToStr( INT32 style = 11 )
{
    static const char   *szMonthsShort[] =
        { "JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC" };
    static const char   *szMonthsFull[] =
        { "January", "February", "March", "April", "May", "June",
          "July", "August", "September", "October", "November", "December" };
    static const char   *szAmPm[] = { "AM", "PM" };
    // the following array is used to map from 24-hour to 12-hour time
    static const INT32  iHr12[] = { 12, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 };

    INT32 year, month, day, hour, minute, second, msec;
    INT32 p = 0;
    static const INT32 iMaxStrLen = 40;

    if (m_szText == NULL)
        m_szText = new char[iMaxStrLen];
    m_szText[0] = 0;

    GetYMDHMS(&year, &month, &day, &hour, &minute, &second, &msec);

    // DATE portion
    switch (style/10)
    {
    case 1:
        // YYYY-MM-DD
        p = sprintf( m_szText, "%04d-%02d-%02d ", year, month, day );
        break;
    case 2:
        // MM/DD/YY
        p = sprintf( m_szText, "%02d/%02d/%02d ", month, day, year%100 );
        break;
    case 3:
        // MM/DD/YYYY
        p = sprintf( m_szText, "%02d/%02d/%04d ", month, day, year );
        break;
    case 4:
        // DD-MON-YYYY
        p = sprintf( m_szText, "%02d-%s-%04d ", day, szMonthsShort[month-1], year );
        break;
    case 5:
        // DD-MON-YY
        p = sprintf( m_szText, "%02d-%s-%02d ", day, szMonthsShort[month-1], year%100 );
        break;
    case 6:
        // MM-DD-YY
        p = sprintf( m_szText, "%02d-%02d-%02d ", month, day, year%100 );
        break;
    case 7:
        // MON DD YYYY
        p = sprintf( m_szText, "%s %02d %04d ", szMonthsShort[month-1], day, year );
        break;
    case 8:
        // Month DD, YYYY
        p = sprintf( m_szText, "%s %02d, %04d ", szMonthsFull[month-1], day, year );
        break;
    }

    // TIME portion
    switch (style%10)
    {
    case 1:
        // HH:MM:SS     (24hr)
        p += sprintf( m_szText+p, "%02d:%02d:%02d", hour, minute, second );
        break;
    case 2:
        // HH:MM:SS.mmm (24hr)
        p += sprintf( m_szText+p, "%02d:%02d:%02d.%03d", hour, minute, second, msec );
        break;
    case 3:
        // HH:MM        (24hr)
        p += sprintf( m_szText+p, "%02d:%02d", hour, minute );
        break;
    case 4:
        // HH:MM:SS [AM|PM]
        p += sprintf( m_szText+p, "%02d:%02d:%02d %s", iHr12[hour], minute, second, szAmPm[hour/12] );
        break;
    case 5:
        // HHH:MM:SS.mmm [AM|PM]
        p += sprintf( m_szText+p, "%02d:%02d:%02d.%03d %s", iHr12[hour], minute, second, msec, szAmPm[hour/12] );
        break;
    case 6:
        // HH:MM [AM|PM]
        p += sprintf( m_szText+p, "%02d:%02d %s", iHr12[hour], minute, szAmPm[hour/12] );
        break;
    }

    // trim trailing blank, if there is one.
    if (p>0 && m_szText[p-1] == ' ')
        m_szText[p-1] = 0;

    return m_szText;
}

// set to current local time
CDateTime::CDateTime(void)
{
    m_szText = NULL;
    SetToCurrent();
}

CDateTime::CDateTime(INT32 dayno)
{
    m_szText = NULL;
    m_dayno = dayno;
    m_msec = 0;
}

CDateTime::CDateTime(INT32 year, INT32 month, INT32 day)
{
    assert( IsValid( year, month, day, 0, 0, 0, 0 ) );

    m_szText = NULL;
    m_dayno = YMDtoDayno(year, month, day);
    m_msec = 0;
}

//Copy constructor
CDateTime::CDateTime(const CDateTime& dt)
{
    m_dayno = dt.m_dayno;
    m_msec = dt.m_msec;
    m_szText = NULL;
}

CDateTime::~CDateTime(void)
{
    if (m_szText)
        delete [] m_szText;
}

CDateTime::CDateTime(INT32 year, INT32 month, INT32 day, INT32 hour, INT32 minute, INT32 second, INT32 msec)
{
    assert( IsValid( year, month, day, hour, minute, second, msec ) );

    m_szText = NULL;
    m_dayno = YMDtoDayno(year, month, day);
    m_msec = ((hour * MinutesPerHour + minute) * SecondsPerMinute + second) * MsPerSecond + msec;
}

CDateTime::CDateTime(TPCE::TIMESTAMP_STRUCT *ts)
{
    assert( IsValid( ts->year, ts->month, ts->day, ts->hour, ts->minute, ts->second, ts->fraction / 1000000 ) );

    m_szText = NULL;
    m_dayno = YMDtoDayno(ts->year, ts->month, ts->day);
    m_msec = ((ts->hour * MinutesPerHour + ts->minute) * SecondsPerMinute + ts->second) * MsPerSecond + ts->fraction / 1000000;
}

// set to current local time
void CDateTime::SetToCurrent(void)
{
#ifdef WIN32
//Windows-specific code to get the current time with 1ms resolution
    SYSTEMTIME  time;

    GetLocalTime(&time);
    m_dayno = YMDtoDayno((INT32)time.wYear, (INT32)time.wMonth, (INT32)time.wDay);
    m_msec = ((time.wHour * MinutesPerHour + time.wMinute) * SecondsPerMinute + time.wSecond) * MsPerSecond + time.wMilliseconds;
#elif (__unix) || (_AIX)
//UNIX-specific code to get the current time with 1ms resolution
    struct timeval tv;
    struct tm ltr;
    gettimeofday(&tv, NULL);
    struct tm* lt = localtime_r(&tv.tv_sec, &ltr);  //expand into year/month/day/...
    // NOTE: 1 is added to tm_mon because it is 0 based, but YMDtoDayno expects it to
    // be 1 based.
    m_dayno = YMDtoDayno(lt->tm_year+1900, lt->tm_mon+1, lt->tm_mday);  // tm_year is based on 1900, not 0.
    m_msec = static_cast<INT32>(((lt->tm_hour * MinutesPerHour + lt->tm_min) * SecondsPerMinute + lt->tm_sec) * MsPerSecond + tv.tv_usec / 1000);
#else
#error No system routine to get time.
#endif
}

void CDateTime::Set(INT32 dayno)
{
    m_dayno = dayno;
    m_msec = 0;
}

void CDateTime::Set(INT32 year, INT32 month, INT32 day)
{
    assert( IsValid( year, month, day, 0, 0, 0, 0 ) );

    m_dayno = YMDtoDayno(year, month, day);
    m_msec = 0;
}

void CDateTime::Set(INT32 year, INT32 month, INT32 day, INT32 hour, INT32 minute, INT32 second, INT32 msec)
{
    assert( IsValid( year, month, day, hour, minute, second, msec ) );

    m_dayno = YMDtoDayno(year, month, day);
    m_msec = ((hour * MinutesPerHour + minute) * SecondsPerMinute + second) * MsPerSecond + msec;
}

void CDateTime::SetHMS(INT32 hour, INT32 minute, INT32 second, INT32 msec)
{
    assert( IsValid( 1, 1, 1, hour, minute, second, msec ) );

    m_msec = ((hour * MinutesPerHour + minute) * SecondsPerMinute + second) * MsPerSecond + msec;
}

// DaynoToYMD converts a day index to
// its corresponding calendar value (mm/dd/yr).  The valid range for days
// is { 0 .. ~3.65M } for dates from 1-Jan-0001 to 31-Dec-9999.
void CDateTime::GetYMD( INT32* year, INT32* month, INT32* day )
{
    INT32 dayno = m_dayno;

    // local variables
    INT32 y, m;

    // days in 1-year period (not including any leap year exceptions) = 365
    static const INT32 dy1 = 365;
    // days in 4-year period (not including 400 and 100-year exceptions) = 1,461
    static const INT32 dy4 = 4 * dy1 + 1;
    // days in 100-year period (not including 400-year exception) = 36,524
    static const INT32 dy100 = 25 * dy4 - 1;
    // days in 400-year period = 146,097
    static const INT32 dy400 = 4 * dy100 + 1;

    y = 1;  // based on year 1 AD
    y += (dayno/dy400) * 400;
    dayno %= dy400;
    if (dayno == dy400-1)  // special case for last day of 400-year leap-year
    {
        y += 399;
        dayno -= 3 * dy100 + 24 * dy4 + 3 * dy1;
    }
    else
    {
        y += (dayno / dy100) * 100;
        dayno %= dy100;
        y += (dayno / dy4) * 4;
        dayno %= dy4;
        if (dayno == dy4 - 1)   // special case for last day of 4-year leap-year
        {
            y += 3;
            dayno -= 3 * dy1;
        }
        else
        {
            y += dayno / dy1;
            dayno %= dy1;
        }
    }

    // determine if this is a leap year
    bool bLeapYear = false;
    if ((y % 4) == 0)       // only years which are multiples of 4 can be leap years
    {
        if ((y % 400) == 0)         // years divisible by 400 are leap years
        {
            bLeapYear = true;
        }
        else
        {
            if ((y % 100) != 0)     // years divisible by 100 but not 400 are not leap years
            {
                bLeapYear = true;
            }
        }
    }

    m = 1;
    dayno++;
    if (bLeapYear)
    {
        while (dayno > monthArrayLY[m-1])
        {
            dayno -= monthArrayLY[m-1];
            m++;
        }
    }
    else
    {
        while (dayno > monthArray[m-1])
        {
            dayno -= monthArray[m-1];
            m++;
        }
    }

    *year = y;
    *month = m;
    *day = dayno;
}

void CDateTime::GetYMDHMS(INT32* year, INT32* month, INT32* day, INT32* hour, INT32* minute, INT32* second, INT32* msec)
{
    INT32 ms = m_msec;

    GetYMD( year, month, day );

    *msec = ms % MsPerSecond;
    ms /= MsPerSecond;
    *second = ms % SecondsPerMinute;
    ms /= SecondsPerMinute;
    *minute = ms % MinutesPerHour;
    *hour = ms / MinutesPerHour;
}

void CDateTime::GetHMS(INT32* hour, INT32* minute, INT32* second, INT32* msec)
{
    INT32 ms = m_msec;

    *msec = ms % MsPerSecond;
    ms /= MsPerSecond;
    *second = ms % SecondsPerMinute;
    ms /= SecondsPerMinute;
    *minute = ms % MinutesPerHour;
    *hour = ms / MinutesPerHour;
}

void CDateTime::GetTimeStamp(TPCE::TIMESTAMP_STRUCT* ts)
{
    INT32 year, month, day, hour, minute, second, msec;

    GetYMDHMS( &year, &month, &day, &hour, &minute, &second, &msec );
    ts->year = (INT16)year;
    ts->month = (UINT16)month;
    ts->day = (UINT16)day;
    ts->hour = (UINT16)hour;
    ts->minute = (UINT16)minute;
    ts->second = (UINT16)second;
    ts->fraction = (UINT32)msec*1000000;    //because "fraction" is 1/billion'th of a second
}

#ifdef COMPILE_ODBC_LOAD
static const INT32 dayno_1Jan1900 = CDateTime::YMDtoDayno( 1900, 1, 1 );

void CDateTime::GetDBDATETIME(DBDATETIME* dt)
{
    dt->dtdays = m_dayno - dayno_1Jan1900;
    dt->dttime = m_msec * 3 / 10;
}
#endif //COMPILE_ODBC_LOAD

void CDateTime::Add(INT32 days, INT32 msec, bool adjust_weekend /* =false */)
{
    if (adjust_weekend)
    {
        days = ((days / DaysPerWorkWeek) * DaysPerWeek) + (days % DaysPerWorkWeek);
    }

    m_dayno += days;

    m_msec += msec;
    m_dayno += m_msec / MsPerDay;
    m_msec %= MsPerDay;
    if (m_msec < 0)
    {
        m_dayno--;
        m_msec += MsPerDay;
    }
}
void CDateTime::AddMinutes(INT32 Minutes)
{
    Add( 0, Minutes * SecondsPerMinute * MsPerSecond);
}
void CDateTime::AddWorkMs(INT64 WorkMs)
{
    INT32 WorkDays  = (INT32)(WorkMs / (INT64)MsPerWorkDay);
    Add( WorkDays, (INT32)(WorkMs % MsPerWorkDay), true );
}
bool CDateTime::operator <(const CDateTime& dt)
{
    return (m_dayno == dt.m_dayno) ? (m_msec < dt.m_msec) : (m_dayno < dt.m_dayno);
}

bool CDateTime::operator <=(const CDateTime& dt)
{
    return (m_dayno == dt.m_dayno) ? (m_msec <= dt.m_msec) : (m_dayno <= dt.m_dayno);
}

namespace TPCE
{

//Need const reference left argument for greater<CDateTime> comparison function
bool operator >(const CDateTime& l_dt, const CDateTime& r_dt)
{
    return (l_dt.m_dayno == r_dt.m_dayno) ? (l_dt.m_msec > r_dt.m_msec) : (l_dt.m_dayno > r_dt.m_dayno);
}

}   // namespace TPCE

bool CDateTime::operator >=(const CDateTime& dt)
{
    return (m_dayno == dt.m_dayno) ? (m_msec >= dt.m_msec) : (m_dayno >= dt.m_dayno);
}

bool CDateTime::operator ==(const CDateTime& dt)
{
    return m_dayno == dt.m_dayno ? m_msec == dt.m_msec : false;
}

// compute the difference between two DateTimes;
// result in seconds
double CDateTime::operator -(const CDateTime& dt)
{
    double dSecs;
    dSecs = (double)((m_dayno - dt.m_dayno) * SecondsPerMinute * MinutesPerHour * HoursPerDay);
    dSecs += (double)(m_msec - dt.m_msec) / MsPerSecondDivisor;
    return dSecs;
}

INT32 CDateTime::DiffInMilliSeconds( const CDateTime& BaseTime )
{
    INT32   mSecs;
    mSecs = (m_dayno - BaseTime.m_dayno ) * MsPerSecond * SecondsPerMinute * MinutesPerHour * HoursPerDay;
    mSecs += (m_msec - BaseTime.m_msec);
    return mSecs;
}

INT32 CDateTime::DiffInMilliSeconds( CDateTime* pBaseTime )
{
    INT32   mSecs;
    mSecs = (m_dayno - pBaseTime->m_dayno ) * MsPerSecond * SecondsPerMinute * MinutesPerHour * HoursPerDay;
    mSecs += (m_msec - pBaseTime->m_msec);
    return mSecs;
}

CDateTime& CDateTime::operator = (const CDateTime& dt)
{
    m_dayno = dt.m_dayno;
    m_msec = dt.m_msec;

    return *this;
}

CDateTime& CDateTime::operator += (const CDateTime& dt)
{
    Add(dt.m_dayno, dt.m_msec);

    return *this;
}
