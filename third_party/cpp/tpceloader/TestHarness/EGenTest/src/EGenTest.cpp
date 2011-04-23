/*
 * Legal Notice
 *
 * This document and associated source code (the "Work") is a part of a
 * benchmark specification maintained by the TPC.
 *
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
 * - Sergey Vasilevskiy
 */

/*
*   Test executable to run two EGen versions in lockstep and detect differences in generated data.
*/

#include "../inc/EGenTestSuites_stdafx.h"

// Generic Error Codes (already defined in EGenLoader_stdafx.h)
/*enum {
    ERROR_BAD_OPTION = 1,
    ERROR_INPUT_FILE,   // 2
    ERROR_INVALID_OPTION_VALUE // 3
};*/

using namespace TPCETest;

// Driver Defaults
TIdent              iStartFromCustomer = TPCE::iDefaultStartFromCustomer;
TIdent              iCustomerCount = TPCE::iDefaultCustomerCount;         // # of customers for this instance
TIdent              iTotalCustomerCount = TPCE::iDefaultCustomerCount;    // total number of customers in the database
int                 iLoadUnitSize = TPCE::iDefaultLoadUnitSize;           // # of customers in one load unit
int                 iScaleFactor = 500;                                   // # of customers for 1 tpsE
int                 iDaysOfInitialTrades = 300;

char                szRefInDir[RefTPCE::iMaxPath] = "../Reference/flat_in/";
char                szInDir[TPCE::iMaxPath] = "../../flat_in/";

eTestSuite          iTestSuite = eTestSuiteAll; // Test suite number (0 means run all suites)

int                 iCEIterationCount = 100000;
int                 iDMIterationCount = 10000;

/*
 * Prints program usage to std error.
 */
void Usage()
{
  fprintf( stderr, "Usage:\n" );
  fprintf( stderr, "EGenLoader [options] \n\n" );
  fprintf( stderr, " Where\n" );
  fprintf( stderr, "  Option                       Default     Description\n" );
  fprintf( stderr, "   -b number                   %" PRId64 "           Beginning customer ordinal position\n", iStartFromCustomer);
  fprintf( stderr, "   -c number                   %" PRId64 "        Number of customers (for this instance)\n", iCustomerCount );
  fprintf( stderr, "   -t number                   %" PRId64 "        Number of customers (total in the database)\n", iTotalCustomerCount );
  fprintf( stderr, "   -f number                   %d         Scale factor (customers per 1 tpsE)\n", iScaleFactor );
  fprintf( stderr, "   -w number                   %d         Number of Workdays (8-hour days) of \n", iDaysOfInitialTrades );
  fprintf( stderr, "                                           initial trades to populate\n" );
  fprintf( stderr, "   -r dir                      %-11s Directory for Reference input files\n", szRefInDir );
  fprintf( stderr, "   -i dir                      %-11s Directory for input files\n", szInDir );
  fprintf( stderr, "   -s [0,1,2]                    0      Test suite 1 or 2 (0 means both)\n" );
}

void ParseCommandLine( int argc, char *argv[] )
{
int   arg;
char  *sp;
char  *vp;

  /*
   *  Scan the command line arguments
   */
  for ( arg = 1; arg < argc; ++arg ) {

    /*
     *  Look for a switch
     */
    sp = argv[arg];
    if ( *sp == '-' ) {
      ++sp;
    }
    *sp = (char)tolower( *sp );

    /*
     *  Find the switch's argument.  It is either immediately after the
     *  switch or in the next argv
     */
    vp = sp + 1;
    // Allow for switched that don't have any parameters.
    // Need to check that the next argument is in fact a parameter
    // and not the next switch that starts with '-'.
    //
    if ( (*vp == 0) && ((arg + 1) < argc) && (argv[arg + 1][0] != '-') )
    {
        vp = argv[++arg];
    }

    /*
     *  Parse the switch
     */
    switch ( *sp ) {
      case 'b':
        sscanf(vp, "%"PRId64, &iStartFromCustomer);
        if (iStartFromCustomer <= 0)
        {   // set back to default
            // either number parsing was unsuccessful
            // or a bad value was specified
            iStartFromCustomer = TPCE::iDefaultStartFromCustomer;
        }
        break;
      case 'c':
        sscanf(vp, "%"PRId64, &iCustomerCount);
        break;
      case 't':
        sscanf(vp, "%"PRId64, &iTotalCustomerCount);
        break;
      case 'f':
        iScaleFactor = atoi( vp );
        break;
      case 'w':
        iDaysOfInitialTrades = atoi( vp );
        break;

      case 'i': // Location of input files.
          strncpy(szInDir, vp, sizeof(szInDir));
          if(( '/' != szInDir[ strlen(szInDir) - 1 ] ) && ( '\\' != szInDir[ strlen(szInDir) - 1 ] ))
          {
              strncat( szInDir, "/", sizeof(szInDir) );
          }
          break;

      case 'r': // Location of Reference input files.
          strncpy(szRefInDir, vp, sizeof(szRefInDir));
          if(( '/' != szRefInDir[ strlen(szRefInDir) - 1 ] ) && ( '\\' != szRefInDir[ strlen(szRefInDir) - 1 ] ))
          {
              strncat( szRefInDir, "/", sizeof(szRefInDir) );
          }
          break;

      case 's': // Test suite number.
          iTestSuite = (eTestSuite)atoi(vp);
          break;
      
      default:
        Usage();
        fprintf( stderr, "Error: Unrecognized option: %s\n",sp);
        exit( ERROR_BAD_OPTION );
    }
  }

}

/*
* This function validates EGenTest parameters that may have been
* specified on the command line. It's purpose is to prevent passing of
* parameter values that would cause the loaders to produce incorrect data.
*
* Condition: needs to be called after ParseCommandLine.
*
* PARAMETERS:
*       NONE
*
* RETURN:
*       true                    - if all parameters are valid
*       false                   - if at least one parameter is invalid
*/
bool ValidateParameters()
{
    bool bRet = true;

    // Starting customer must be a non-zero integral multiple of load unit size + 1.
    //
    if ((iStartFromCustomer % iLoadUnitSize) != 1)
    {
        cout << "The specified starting customer (-b " << iStartFromCustomer
            << ") must be a non-zero integral multiple of the load unit size ("
            << iLoadUnitSize << ") + 1." << endl;

        bRet = false;
    }

    // Total number of customers in the database cannot be less
    // than the number of customers for this loader instance
    //
    if (iTotalCustomerCount < iStartFromCustomer + iCustomerCount - 1)
    {
        iTotalCustomerCount = iStartFromCustomer + iCustomerCount - 1;
    }

    // Customer count must be a non-zero integral multiple of load unit size.
    //
    if ((iLoadUnitSize > iCustomerCount) || (0 != iCustomerCount % iLoadUnitSize))
    {
        cout << "The specified customer count (-c " << iCustomerCount
            << ") must be a non-zero integral multiple of the load unit size ("
            << iLoadUnitSize << ")." << endl;

        bRet = false;
    }

    // Total customer count must be a non-zero integral multiple of load unit size.
    //
    if ((iLoadUnitSize > iTotalCustomerCount) || (0 != iTotalCustomerCount % iLoadUnitSize))
    {
        cout << "The total customer count (-t " << iTotalCustomerCount
            << ") must be a non-zero integral multiple of the load unit size ("
            << iLoadUnitSize << ")." << endl;

        bRet = false;
    }

    // Completed trades in 8 hours must be a non-zero integral multiple of 100
    // so that exactly 1% extra trade ids can be assigned to simulate aborts.
    //
    // Note: need to check two times - once using Reference constants and once using Current constants.
    //
    if ((INT64)(RefTPCE::HoursPerWorkDay * RefTPCE::SecondsPerHour * iLoadUnitSize / iScaleFactor) % 100 != 0)
    {
        cout << "Incompatible value for Reference Scale Factor (-f) specified." << endl;
        cout << RefTPCE::HoursPerWorkDay << " * " << RefTPCE::SecondsPerHour << " * Load Unit Size (" << iLoadUnitSize
            << ") / Scale Factor (" << iScaleFactor
            << ") must be integral multiple of 100." << endl;

        bRet = false;
    }

    if ((INT64)(TPCE::HoursPerWorkDay * TPCE::SecondsPerHour * iLoadUnitSize / iScaleFactor) % 100 != 0)
    {
        cout << "Incompatible value for Current Scale Factor (-f) specified." << endl;
        cout << TPCE::HoursPerWorkDay << " * " << TPCE::SecondsPerHour << " * Load Unit Size (" << iLoadUnitSize
            << ") / Scale Factor (" << iScaleFactor
            << ") must be integral multiple of 100." << endl;

        bRet = false;
    }

    if (iDaysOfInitialTrades <= 0)
    {
        cout << "The specified number of 8-Hour Workdays (-w "
            << (iDaysOfInitialTrades) << ") must be non-zero." << endl;

        bRet = false;
    }

    return bRet;
}

int main(int argc, char* argv[])
{
    RefTPCE::CInputFiles                    RefInputFiles;
    TPCE::CInputFiles                       inputFiles;
    TPCE::CGenerateAndLoadStandardOutput    Output;
    RefTPCE::CLogFormatTab                  refFmt;
    TPCE::CLogFormatTab                     fmt;
    RefTPCE::CDateTime                      StartTime;
    RefTPCE::CDateTime                      EndTime;
    double                                  fElapsed;
    char                                    szLogFileName[64];

    //strncpy(szRefInDir, FLAT_REF_IN_PATH, sizeof(szRefInDir)-1);
    //strncpy(szInDir, FLAT_IN_PATH, sizeof(szInDir)-1);

    // Get command line options.
    //
    ParseCommandLine(argc, argv);

    // Validate global parameters that may have been modified on
    // the command line.
    //
    if (!ValidateParameters())
    {
        return ERROR_INVALID_OPTION_VALUE;  // exit from the loader returning a non-zero code
    }

    // Create a unique log name for this loader instance.
    // Simultaneous loader instances are to be run with
    // disjoint customer ranges, so use this fact to
    // construct a unique name.
    //
    sprintf(&szLogFileName[0], "EGenTestRefFrom%" PRId64 "To%" PRId64 ".log",
        iStartFromCustomer, (iStartFromCustomer + iCustomerCount)-1);

    // Create log formatter and logger instance
    //
    RefTPCE::CEGenLogger refLog(RefTPCE::eDriverEGenLoader, 0, szLogFileName, &refFmt);

    //  Same for EGenLogger interface from the Current EGen.
    //
    sprintf(&szLogFileName[0], "EGenTestFrom%" PRId64 "To%" PRId64 ".log",
        iStartFromCustomer, (iStartFromCustomer + iCustomerCount)-1);

    // Create log formatter and logger instance
    //
    TPCE::CEGenLogger log(TPCE::eDriverEGenLoader, 0, szLogFileName, &fmt);

    try {
        // Load all of the input files into memory.
        RefInputFiles.Initialize(RefTPCE::eDriverEGenLoader, iTotalCustomerCount, iTotalCustomerCount, szRefInDir);
        inputFiles.Initialize(TPCE::eDriverEGenLoader, iTotalCustomerCount, iTotalCustomerCount, szInDir);

        // Let the user know what settings will be used.
        cout<<endl<<"Using the following settings."<<endl<<endl;
        cout<<"\tReference In Directory:\t"<<szRefInDir<<endl;
        cout<<"\tIn Directory:\t\t"<<szInDir<<endl;
        cout<<"\tStart From Customer:\t"<<iStartFromCustomer<<endl;
        cout<<"\tCustomer Count:\t\t"<<iCustomerCount<<endl;
        cout<<"\tTotal customers:\t"<<iTotalCustomerCount<<endl;
        cout<<"\tLoad Unit:\t\t"<<iLoadUnitSize<<endl;
        cout<<"\tScale Factor:\t\t"<<iScaleFactor<<endl;
        cout<<"\tInitial Trade Days:\t"<<iDaysOfInitialTrades<<endl;
        cout<<"\tSuite to run:\t\t";
        if (iTestSuite == eTestSuiteAll)
        { 
            cout<<"All"<<endl;
        }
        else
        {
            cout<<iTestSuite<<endl;
        }
        cout<<endl;

        cout<<"Comparing reference\t";
        RefTPCE::PrintEGenVersion();
        cout<<"to:\t\t\t";
        TPCE::PrintEGenVersion();
        cout<<endl;

        StartTime.SetToCurrent();   // measure start

        if (iTestSuite == eTestSuiteAll || iTestSuite == eTestSuiteOne)
        {
            CEGenTestSuiteOne   Suite(
                RefInputFiles,
                inputFiles,
                iCustomerCount,
                iStartFromCustomer,
                iTotalCustomerCount,
                iLoadUnitSize,
                iScaleFactor,
                iDaysOfInitialTrades,
                &log,
                &Output,
                szRefInDir,
                szInDir);

            Suite.Run();
        }

        if (iTestSuite == eTestSuiteAll || iTestSuite == eTestSuiteTwo)
        {
            CEGenTestSuiteTwo   Suite(
                RefInputFiles,
                inputFiles,
                iCustomerCount,
                iStartFromCustomer,
                iTotalCustomerCount,
                iLoadUnitSize,
                iScaleFactor,
                iDaysOfInitialTrades,
                &refLog,
                &log,
                &Output,
                iCEIterationCount,
                iDMIterationCount);

            Suite.Run();
        }
    }    
    // operator new will throw std::bad_alloc exception if there is no sufficient memory for the request.
    //
    catch (std::bad_alloc&)
    {
        cout<<endl<<endl<<"*** Out of memory ***"<<endl;
        return 2;
    }
    catch (std::exception& err)
    {
        cout << endl << endl <<"Caught Exception: " << endl << err.what() << endl;
    }
    catch (...)
    {
        cout << endl << endl <<"Caught Unknown Exception" << endl;
    }

    EndTime.SetToCurrent(); // measure end

    fElapsed = EndTime-StartTime;

    int iHours = static_cast<int>(fElapsed / RefTPCE::SecondsPerHour);
    int iMinutes = static_cast<int>((fElapsed - (iHours * RefTPCE::SecondsPerHour)) / RefTPCE::SecondsPerMinute);
    int iSeconds = static_cast<int>(fElapsed - (iHours * RefTPCE::SecondsPerHour) - (iMinutes * RefTPCE::SecondsPerMinute));

    cout << "Runtime: " << iHours << "h. " << iMinutes << "m. " << iSeconds << "sec." << endl;

    return 0;
}

