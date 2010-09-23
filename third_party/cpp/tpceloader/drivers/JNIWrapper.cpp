/***************************************************************************
 *  Copyright (C) 2009 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  Andy Pavlo (pavlo@cs.brown.edu)                                        *
 *  http://www.cs.brown.edu/~pavlo/                                        *
 *                                                                         *
 *  Based DBT5 (2006):                                                     *
 *  Rilson Nascimento                                                      *
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

#include <jni.h>
#include <iostream>

#include <MiscConsts.h>

#include "edu_brown_benchmark_tpce_EGenClientDriver.h"
#include "JNIUtils.h"
#include "ClientDriver.h"

using namespace std;
using namespace TPCE;

#define castToDriver(x) reinterpret_cast<ClientDriver*>((x));

JNIEXPORT jlong JNICALL Java_edu_brown_benchmark_tpce_EGenClientDriver_initialize(
    JNIEnv *env, jobject obj,
    jstring dataPath, jint configuredCustomerCount, jint totalCustomerCount, jint scaleFactor, jint initialDays) {

    // Instantiate a new ClientDriver 
    jobject java_ee = env->NewGlobalRef(obj);
    if (java_ee == NULL) {
        assert(!"Failed to allocate global reference to java EE.");
        return (0);
    }
    
    const char* utf_chars = env->GetStringUTFChars(dataPath, NULL);
    string str_dataPath(utf_chars);
    env->ReleaseStringUTFChars(dataPath, utf_chars);
    
    ClientDriver *driver = new ClientDriver(str_dataPath, static_cast<int>(configuredCustomerCount), static_cast<int>(totalCustomerCount), static_cast<int>(scaleFactor), static_cast<int>(initialDays));
    assert(driver != NULL);
    
    checkException(env);

    return reinterpret_cast<jlong>(driver);
}

/*
 * Class:     edu_brown_benchmark_tpce_EGenClientDriver
 * Method:    egenBrokerVolume
 * Signature: (J)[Ljava/lang/Object;
 */
JNIEXPORT jobjectArray JNICALL Java_edu_brown_benchmark_tpce_EGenClientDriver_egenBrokerVolume(
    JNIEnv *env, jobject obj,
    jlong driver_ptr) {
    
    ClientDriver *driver = castToDriver(driver_ptr);
    TBrokerVolumeTxnInput params = driver->generateBrokerVolumeInput();
    jobjectArray result = makeObjectArray(env, "java/lang/Object", 2);
    
    //
    // Make an array of Strings for the broker_list
    //
    jobjectArray broker_list = makeObjectArray(env, "java/lang/String", max_broker_list_len);
    #ifdef DEBUG
    fprintf(stderr, "broker_list:\n");
    #endif
    for (int i = 0; i < max_broker_list_len; i++) {
        #ifdef DEBUG
        fprintf(stderr, "    [%02d]:        %s\n", i, params.broker_list[i]);
        #endif
        env->SetObjectArrayElement(broker_list, i, makeString(env, params.broker_list[i]));
    } // FOR
    #ifdef DEBUG
    fprintf(stderr, "sector_name:     %s\n", params.sector_name);
    #endif
    
    int i = 0;
    env->SetObjectArrayElement(result, i++, broker_list);
    env->SetObjectArrayElement(result, i++, makeString(env, params.sector_name));
    
    assert(i == env->GetArrayLength(result));
    checkException(env);
    
    return (result);
}

/*
 * Class:     edu_brown_benchmark_tpce_EGenClientDriver
 * Method:    egenCustomerPosition
 * Signature: (J)[Ljava/lang/Object;
 */
JNIEXPORT jobjectArray JNICALL Java_edu_brown_benchmark_tpce_EGenClientDriver_egenCustomerPosition(
    JNIEnv *env, jobject obj,
    jlong driver_ptr) {
    
    ClientDriver *driver = castToDriver(driver_ptr);
    TCustomerPositionTxnInput params = driver->generateCustomerPositionInput();
    jobjectArray result = makeObjectArray(env, "java/lang/Object", 4);

    #ifdef DEBUG
    fprintf(stderr, "acct_id_idx:  %lld\n", params.acct_id_idx);
    fprintf(stderr, "cust_id:      %lld\n", params.cust_id);
    fprintf(stderr, "get_history:  %d\n", params.get_history);
    fprintf(stderr, "tax_id:       %s\n", params.tax_id);
    #endif
    
    int i = 0;
    env->SetObjectArrayElement(result, i++, makeLong(env, params.acct_id_idx));
    env->SetObjectArrayElement(result, i++, makeLong(env, params.cust_id));
    env->SetObjectArrayElement(result, i++, makeLong(env, params.get_history));
    env->SetObjectArrayElement(result, i++, makeString(env, params.tax_id));
    
    assert(i == env->GetArrayLength(result));
    checkException(env);
    
    return (result);
}

/*
 * Class:     edu_brown_benchmark_tpce_EGenClientDriver
 * Method:    egenMarketWatch
 * Signature: (J)[Ljava/lang/Object;
 */
JNIEXPORT jobjectArray JNICALL Java_edu_brown_benchmark_tpce_EGenClientDriver_egenMarketWatch(
    JNIEnv *env, jobject obj,
    jlong driver_ptr) {
    
    ClientDriver *driver = castToDriver(driver_ptr);
    TMarketWatchTxnInput params = driver->generateMarketWatchInput();
    jobjectArray result = makeObjectArray(env, "java/lang/Object", 5);

    #ifdef DEBUG
    fprintf(stderr, "acct_id:       %lld\n", params.acct_id);
    fprintf(stderr, "c_id:          %lld\n", params.c_id);
    fprintf(stderr, "ending_co_id:  %lld\n", params.ending_co_id);
    fprintf(stderr, "starting_co_id:%lld\n", params.starting_co_id);
    // fprintf(stderr, "start_day:     %s\n", "----");
    fprintf(stderr, "industry_name: %s\n", params.industry_name);
    #endif
    
    int i = 0;
    env->SetObjectArrayElement(result, i++, makeLong(env, params.acct_id));
    env->SetObjectArrayElement(result, i++, makeLong(env, params.c_id));
    env->SetObjectArrayElement(result, i++, makeLong(env, params.ending_co_id));
    env->SetObjectArrayElement(result, i++, makeLong(env, params.starting_co_id));
    // This gets created but the stored procedure never uses it??
    // env->SetObjectArrayElement(result, i++, makeLong(env, params.start_day));
    env->SetObjectArrayElement(result, i++, makeString(env, params.industry_name));
    
    assert(i == env->GetArrayLength(result));
    checkException(env);
    
    return (result);
}

/*
 * Class:     edu_brown_benchmark_tpce_EGenClientDriver
 * Method:    egenSecurityDetail
 * Signature: (J)[Ljava/lang/Object;
 */
JNIEXPORT jobjectArray JNICALL Java_edu_brown_benchmark_tpce_EGenClientDriver_egenSecurityDetail(
    JNIEnv *env, jobject obj,
    jlong driver_ptr) {
    
    ClientDriver *driver = castToDriver(driver_ptr);
    TSecurityDetailTxnInput params = driver->generateSecurityDetailInput();
    jobjectArray result = makeObjectArray(env, "java/lang/Object", 4);

    #ifdef DEBUG
    fprintf(stderr, "max_rows_to_return: %d\n", params.max_rows_to_return);
    fprintf(stderr, "access_lob_flag:    %d\n", params.access_lob_flag);
    fprintf(stderr, "start_day:          %s\n", "----");
    fprintf(stderr, "symbol:             %s\n", params.symbol);
    #endif
    
    int i = 0;
    env->SetObjectArrayElement(result, i++, makeLong(env, params.max_rows_to_return));
    env->SetObjectArrayElement(result, i++, makeLong(env, params.access_lob_flag));
    env->SetObjectArrayElement(result, i++, makeDate(env, params.start_day));
    env->SetObjectArrayElement(result, i++, makeString(env, params.symbol));
    
    assert(i == env->GetArrayLength(result));
    checkException(env);
    
    return (result);
}

/*
 * Class:     edu_brown_benchmark_tpce_EGenClientDriver
 * Method:    egenTradeLookup
 * Signature: (J)[Ljava/lang/Object;
 */
JNIEXPORT jobjectArray JNICALL Java_edu_brown_benchmark_tpce_EGenClientDriver_egenTradeLookup(
    JNIEnv *env, jobject obj,
    jlong driver_ptr) {
    
    ClientDriver *driver = castToDriver(driver_ptr);
    TTradeLookupTxnInput params = driver->generateTradeLookupInput();
    jobjectArray result = makeObjectArray(env, "java/lang/Object", 8);

    //
    // Make an array of Strings for trade_id
    //
    jobjectArray trade_ids = makeObjectArray(env, "java/lang/Long", TradeLookupFrame1MaxRows);
    #ifdef DEBUG
    fprintf(stderr, "trade_id:\n");
    #endif
    for (int i = 0; i < TradeLookupFrame1MaxRows; i++) {
        #ifdef DEBUG
        fprintf(stderr, "    [%02d]:        %lld\n", i, params.trade_id[i]);
        #endif
        env->SetObjectArrayElement(trade_ids, i, makeLong(env, params.trade_id[i]));
    } // FOR

    #ifdef DEBUG
    fprintf(stderr, "%-15s %lld\n", "acct_id", params.acct_id);
    fprintf(stderr, "%-15s %lld\n", "max_acct_id", params.max_acct_id);
    fprintf(stderr, "%-15s %lld\n", "frame_to_execute", params.frame_to_execute);
    fprintf(stderr, "%-15s %lld\n", "max_trades", params.max_trades);
    fprintf(stderr, "%-15s %s\n", "end_trade_dts", "----");
    fprintf(stderr, "%-15s %s\n", "start_trade_dts", "----");
    fprintf(stderr, "%-15s %s\n", "symbol", params.symbol);
    #endif

    int i = 0;
    env->SetObjectArrayElement(result, i++, trade_ids);
    env->SetObjectArrayElement(result, i++, makeLong(env, params.acct_id));
    env->SetObjectArrayElement(result, i++, makeLong(env, params.max_acct_id));
    env->SetObjectArrayElement(result, i++, makeLong(env, params.frame_to_execute));
    env->SetObjectArrayElement(result, i++, makeLong(env, params.max_trades));
    env->SetObjectArrayElement(result, i++, makeDate(env, params.end_trade_dts));
    env->SetObjectArrayElement(result, i++, makeDate(env, params.start_trade_dts));
    env->SetObjectArrayElement(result, i++, makeString(env, params.symbol));
    
    assert(i == env->GetArrayLength(result));
    checkException(env);
    
    return (result);
}

/*
 * Class:     edu_brown_benchmark_tpce_EGenClientDriver
 * Method:    egenTradeOrder
 * Signature: (J)[Ljava/lang/Object;
 */
JNIEXPORT jobjectArray JNICALL Java_edu_brown_benchmark_tpce_EGenClientDriver_egenTradeOrder(
    JNIEnv *env, jobject obj,
    jlong driver_ptr) {
    
    
    ClientDriver *driver = castToDriver(driver_ptr);
    INT32   iTradeType;
    bool    bExecutorIsAccountOwner;
    TTradeOrderTxnInput params = driver->generateTradeOrderInput(iTradeType, bExecutorIsAccountOwner);
    
    // We now need to convert the struct into a object array
    jobjectArray result = makeObjectArray(env, "java/lang/Object", 15);

    #ifdef DEBUG
    fprintf(stderr, "requested_price: %g\n", params.requested_price);
    fprintf(stderr, "acct_id:         %lld\n", params.acct_id);
    fprintf(stderr, "is_lifo:         %d\n", params.is_lifo);
    fprintf(stderr, "roll_it_back:    %d\n", params.roll_it_back);
    fprintf(stderr, "trade_qty:       %d\n", params.trade_qty);
    fprintf(stderr, "type_is_margin:  %d\n", params.type_is_margin);
    fprintf(stderr, "co_name:         %s\n", params.co_name);
    fprintf(stderr, "exec_f_name:     %s\n", params.exec_f_name);
    fprintf(stderr, "exec_l_name:     %s\n", params.exec_l_name);
    fprintf(stderr, "exec_tax_id:     %s\n", params.exec_tax_id);
    fprintf(stderr, "issue:           %s\n", params.issue);
    fprintf(stderr, "st_pending_id:   %s\n", params.st_pending_id);
    fprintf(stderr, "st_submitted_id: %s\n", params.st_submitted_id);
    fprintf(stderr, "symbol:          %s\n", params.symbol);
    fprintf(stderr, "trade_type_id:   %s\n", params.trade_type_id);
    #endif
    
    int i = 0;
    env->SetObjectArrayElement(result, i++, makeDouble(env, params.requested_price));
    env->SetObjectArrayElement(result, i++, makeLong(env, params.acct_id));
    env->SetObjectArrayElement(result, i++, makeLong(env, params.is_lifo));
    env->SetObjectArrayElement(result, i++, makeLong(env, params.roll_it_back));
    env->SetObjectArrayElement(result, i++, makeLong(env, params.trade_qty));
    env->SetObjectArrayElement(result, i++, makeLong(env, params.type_is_margin));
    env->SetObjectArrayElement(result, i++, makeString(env, params.co_name));
    env->SetObjectArrayElement(result, i++, makeString(env, params.exec_f_name));
    env->SetObjectArrayElement(result, i++, makeString(env, params.exec_l_name));
    env->SetObjectArrayElement(result, i++, makeString(env, params.exec_tax_id));
    env->SetObjectArrayElement(result, i++, makeString(env, params.issue));
    env->SetObjectArrayElement(result, i++, makeString(env, params.st_pending_id));
    env->SetObjectArrayElement(result, i++, makeString(env, params.st_submitted_id));
    env->SetObjectArrayElement(result, i++, makeString(env, params.symbol));
    env->SetObjectArrayElement(result, i++, makeString(env, params.trade_type_id));
    
    // Not sure if I need to pass these too just yet...
    //env->SetObjectArrayElement(result, i++, makeLong(env, iTradeType));
    //env->SetObjectArrayElement(result, i++, makeLong(env, bExecutorIsAccountOwner));
        
    assert(i == env->GetArrayLength(result));
    checkException(env);

    return (result);
}

/*
 * Class:     edu_brown_benchmark_tpce_EGenClientDriver
 * Method:    egenTradeStatus
 * Signature: (J)[Ljava/lang/Object;
 */
JNIEXPORT jobjectArray JNICALL Java_edu_brown_benchmark_tpce_EGenClientDriver_egenTradeStatus(
    JNIEnv *env, jobject obj,
    jlong driver_ptr) {
    
    ClientDriver *driver = castToDriver(driver_ptr);
    TTradeStatusTxnInput params = driver->generateTradeStatusInput();
    jobjectArray result = makeObjectArray(env, "java/lang/Object", 1);

    int i = 0;
    env->SetObjectArrayElement(result, i++, makeLong(env, params.acct_id));
    
    assert(i == env->GetArrayLength(result));
    checkException(env);
    
    return (result);
}


/*
 * Class:     edu_brown_benchmark_tpce_EGenClientDriver
 * Method:    egenTradeUpdate
 * Signature: (J)[Ljava/lang/Object;
 */
JNIEXPORT jobjectArray JNICALL Java_edu_brown_benchmark_tpce_EGenClientDriver_egenTradeUpdate(
    JNIEnv *env, jobject obj,
    jlong driver_ptr) {
    
    ClientDriver *driver = castToDriver(driver_ptr);
    TTradeUpdateTxnInput params = driver->generateTradeUpdateInput();
    jobjectArray result = makeObjectArray(env, "java/lang/Object", 9);

    // trade_id
    jobjectArray trade_ids = makeObjectArray(env, "java/lang/Long", TradeUpdateFrame1MaxRows);
    #ifdef DEBUG
    fprintf(stderr, "trade_id:\n");
    #endif
    for (int i = 0; i < TradeUpdateFrame1MaxRows; i++) {
        #ifdef DEBUG
        fprintf(stderr, "    [%02d]:        %lld\n", i, params.trade_id[i]);
        #endif
        env->SetObjectArrayElement(trade_ids, i, makeLong(env, params.trade_id[i]));
    } // FOR

    #ifdef DEBUG
    fprintf(stderr, "%-15s %lld\n", "acct_id", params.acct_id);
    fprintf(stderr, "%-15s %lld\n", "max_acct_id", params.max_acct_id);
    fprintf(stderr, "%-15s %lld\n", "frame_to_execute", params.frame_to_execute);
    fprintf(stderr, "%-15s %lld\n", "max_trades", params.max_trades);
    fprintf(stderr, "%-15s %lld\n", "max_trades", params.max_updates);
    fprintf(stderr, "%-15s %s\n", "end_trade_dts", "----");
    fprintf(stderr, "%-15s %s\n", "start_trade_dts", "----");
    fprintf(stderr, "%-15s %s\n", "symbol", params.symbol);
    #endif

    int i = 0;
    env->SetObjectArrayElement(result, i++, trade_ids);
    env->SetObjectArrayElement(result, i++, makeLong(env, params.acct_id));
    env->SetObjectArrayElement(result, i++, makeLong(env, params.max_acct_id));
    env->SetObjectArrayElement(result, i++, makeLong(env, params.frame_to_execute));
    env->SetObjectArrayElement(result, i++, makeLong(env, params.max_trades));
    env->SetObjectArrayElement(result, i++, makeLong(env, params.max_updates));
    env->SetObjectArrayElement(result, i++, makeDate(env, params.end_trade_dts));
    env->SetObjectArrayElement(result, i++, makeDate(env, params.start_trade_dts));
    env->SetObjectArrayElement(result, i++, makeString(env, params.symbol));
    
    assert(i == env->GetArrayLength(result));
    checkException(env);
    
    return (result);
}
