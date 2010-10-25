#include <stdio.h>
#include <string>

#include <TxnHarnessStructs.h>

#include "ClientDriver.h"

using namespace std;
using namespace TPCE;

int main(int argc, char* argv[]) {
    string dataPath("../flat_in");
    int configuredCustomerCount = 1000;
    int totalCustomerCount = 1000;
    int scaleFactor = 500;
    int initialDays = 1;
    
//     int P = 1;
//     int Q = 0;
//     int A = 65535;
//     int s = 7;
//     
//     int64_t left = 1l;
//     int64_t right = 36992l;
//     int64_t temp = left | (right << s );
// 
//     int value = (temp % (Q - P + 1) ) + P;
//     fprintf(stderr, "VALUE: %d\n", value);
//     return (0);
    
    ClientDriver driver(dataPath, configuredCustomerCount, totalCustomerCount, scaleFactor, initialDays);
    
    for (int i = 0; i < 10; i++) {
        TBrokerVolumeTxnInput bv_params = driver.generateBrokerVolumeInput();
        TDataMaintenanceTxnInput dm_params = driver.generateDataMaintenanceInput();
        TCustomerPositionTxnInput cp_params = driver.generateCustomerPositionInput();
        TMarketWatchTxnInput mw_params = driver.generateMarketWatchInput();
        TSecurityDetailTxnInput sd_params = driver.generateSecurityDetailInput();
        TTradeCleanupTxnInput tc_params = driver.generateTradeCleanupInput();
        TTradeLookupTxnInput tl_params = driver.generateTradeLookupInput();
    
        fprintf(stderr, "\n-----------------------------------------------\n\n");
    
/*        fprintf(stderr, "acct_id:      %ld\n", dm_params.acct_id);
        fprintf(stderr, "c_id:         %ld\n", dm_params.c_id);
        fprintf(stderr, "co_id:        %ld\n", dm_params.co_id);
        fprintf(stderr, "day_of_month: %d\n", dm_params.day_of_month);
        fprintf(stderr, "vol_incr:     %d\n", dm_params.vol_incr);
        fprintf(stderr, "symbol:       %s\n", dm_params.symbol);
        fprintf(stderr, "table_name:   %s\n", dm_params.table_name);
        fprintf(stderr, "tx_id:        %s\n", dm_params.tx_id);*/
        
        fprintf(stderr, "start_trade_id:     %ld\n", tc_params.start_trade_id);
        fprintf(stderr, "st_canceled_id:     %s\n", tc_params.st_canceled_id);
        fprintf(stderr, "st_pending_id:      %s\n", tc_params.st_pending_id);
        fprintf(stderr, "st_submitted_id:    %s\n", tc_params.st_submitted_id);
    }

/*    fprintf(stderr, "trade_id:\n");
    for (int i = 0; i < TradeLookupFrame1MaxRows; i++) {
        fprintf(stderr, "    [%02d]:        %ld\n", i, params.trade_id[i]);
    } // FOR

    fprintf(stderr, "%-15s %ld\n", "acct_id", params.acct_id);
    fprintf(stderr, "%-15s %ld\n", "max_acct_id", params.max_acct_id);
    fprintf(stderr, "%-15s %ld\n", "frame_to_execute", params.frame_to_execute);
    fprintf(stderr, "%-15s %ld\n", "max_trades", params.max_trades);
    fprintf(stderr, "%-15s %s\n", "end_trade_dts", "----");
    fprintf(stderr, "%-15s %s\n", "start_trade_dts", "----");
    fprintf(stderr, "%-15s %s\n", "symbol", params.symbol);*/
    
    return (0);
}