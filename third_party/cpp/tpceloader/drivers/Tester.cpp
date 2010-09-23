#include <stdio.h>
#include <string>

#include <TxnHarnessStructs.h>

#include "ClientDriver.h"

using namespace std;
using namespace TPCE;

int main(int argc, char* argv[]) {
    string dataPath("/home/pavlo/Documents/H-Store/SVN-Brown/src/obj/release/tpceloader/flat_in");
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
    TBrokerVolumeTxnInput bv_params = driver.generateBrokerVolumeInput();
    TCustomerPositionTxnInput cp_params = driver.generateCustomerPositionInput();
    TMarketWatchTxnInput mw_params = driver.generateMarketWatchInput();
    TSecurityDetailTxnInput sd_params = driver.generateSecurityDetailInput();
    TTradeLookupTxnInput params = driver.generateTradeLookupInput();

    fprintf(stderr, "trade_id:\n");
    for (int i = 0; i < TradeLookupFrame1MaxRows; i++) {
        fprintf(stderr, "    [%02d]:        %lld\n", i, params.trade_id[i]);
    } // FOR

    fprintf(stderr, "%-15s %lld\n", "acct_id", params.acct_id);
    fprintf(stderr, "%-15s %lld\n", "max_acct_id", params.max_acct_id);
    fprintf(stderr, "%-15s %lld\n", "frame_to_execute", params.frame_to_execute);
    fprintf(stderr, "%-15s %lld\n", "max_trades", params.max_trades);
    fprintf(stderr, "%-15s %s\n", "end_trade_dts", "----");
    fprintf(stderr, "%-15s %s\n", "start_trade_dts", "----");
    fprintf(stderr, "%-15s %s\n", "symbol", params.symbol);
    
    return (0);
}