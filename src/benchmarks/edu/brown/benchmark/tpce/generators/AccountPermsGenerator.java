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

package edu.brown.benchmark.tpce.generators;

import org.voltdb.catalog.Table;

import edu.brown.benchmark.tpce.TPCEConstants;
import edu.brown.benchmark.tpce.generators.TPCEGenerator.InputFile;
import edu.brown.benchmark.tpce.util.EGenRandom;

public class AccountPermsGenerator extends TableGenerator {
	public final static int percentAccountAdditionalPermissions_0 = 60;
	public final static int percentAccountAdditionalPermissions_1 = 38;
	public final static int percentAccountAdditionalPermissions_2 = 2;
	public final static long accountPermissionIDRange = 4024L * 1024 * 1024 - TPCEConstants.DEFAULT_START_CUSTOMER_ID;

    private final CustomerAccountsGenerator accsGenerator;
    private final EGenRandom rnd;
    private final PersonHandler person;
    
    private int permsGenerated;
    private int permsToGenerate;
    private long currentAccId;
    public long[] permCids = new long[3];
    private final static String[] permACLs = {"000", "001", "011"};
    
    public AccountPermsGenerator(Table catalog_tbl, TPCEGenerator generator) {
        super(catalog_tbl, generator);
        
        accsGenerator = (CustomerAccountsGenerator)generator.getTableGen(TPCEConstants.TABLENAME_CUSTOMER_ACCOUNT, null); // just for generating ids
        
        rnd = new EGenRandom(EGenRandom.RNG_SEED_TABLE_DEFAULT);
        
        person = new PersonHandler(generator.getInputFile(InputFile.LNAME), generator.getInputFile(InputFile.FEMFNAME),
                generator.getInputFile(InputFile.MALEFNAME));
    }
    
    private void switchAccount() {
        currentAccId = accsGenerator.generateAccountId();
        permCids[0] = accsGenerator.getCurrentCid();
        
        permsGenerated = 0;
        permsToGenerate = getNumPermsForAcc(currentAccId);
        
        generateCids();
    }
    
    public int getNumPermsForAcc(long accId) {
        long oldSeed = rnd.getSeed();
        
        rnd.setSeedNth(EGenRandom.RNG_SEED_BASE_NUMBER_OF_ACCOUNT_PERMISSIONS, accId);
        int perc = rnd.rndPercentage();
        rnd.setSeed(oldSeed);
        
        if (perc <= percentAccountAdditionalPermissions_0) {
            return 1;   //60% of accounts have just the owner row permissions
        }
        else if (perc <= percentAccountAdditionalPermissions_0 + percentAccountAdditionalPermissions_1) {
            return 2;   //38% of accounts have one additional permission row
        }
        else {
            return 3;   //2% of accounts have two additional permission rows
        }
    }
    
    private void generateCids() {
        if (permsToGenerate == 1) { // only the customer himself has permissions
            return;
        }
        
        long oldSeed = rnd.getSeed();

        rnd.setSeedNth(EGenRandom.RNG_SEED_BASE_CID_FOR_PERMISSION1, currentAccId);
        permCids[1] = rnd.int64RangeExclude(TPCEConstants.DEFAULT_START_CUSTOMER_ID,
                TPCEConstants.DEFAULT_START_CUSTOMER_ID + accountPermissionIDRange, permCids[0]);
        
        // do we need to generate another one?
        if (permsToGenerate == 3) {
            rnd.setSeedNth(EGenRandom.RNG_SEED_BASE_CID_FOR_PERMISSION2, currentAccId);
            
            // we do not want to generate the same cid as before -- thus, 'while'
            do {
                permCids[2] = rnd.int64RangeExclude(TPCEConstants.DEFAULT_START_CUSTOMER_ID,
                        TPCEConstants.DEFAULT_START_CUSTOMER_ID + accountPermissionIDRange, permCids[0]);
            } while (permCids[1] == permCids[2]);
        }
        
        rnd.setSeed(oldSeed);
    }
    public void generateCids(long customerID, int additionalPerms, long acct_id) {
        if (additionalPerms == 1) { // only the customer himself has permissions
            return;
        }
        
        long oldSeed = rnd.getSeed();

        rnd.setSeedNth(EGenRandom.RNG_SEED_BASE_CID_FOR_PERMISSION1, acct_id);
        permCids[1] = rnd.int64RangeExclude(TPCEConstants.DEFAULT_START_CUSTOMER_ID,
                TPCEConstants.DEFAULT_START_CUSTOMER_ID + accountPermissionIDRange, customerID);
        
        // do we need to generate another one?
        if (additionalPerms == 3) {
            rnd.setSeedNth(EGenRandom.RNG_SEED_BASE_CID_FOR_PERMISSION2, acct_id);
            
            // we do not want to generate the same cid as before -- thus, 'while'
            do {
                permCids[2] = rnd.int64RangeExclude(TPCEConstants.DEFAULT_START_CUSTOMER_ID,
                        TPCEConstants.DEFAULT_START_CUSTOMER_ID + accountPermissionIDRange, customerID);
            } while (permCids[1] == permCids[2]);
        }
        
        rnd.setSeed(oldSeed);
    }

    @Override
    public boolean hasNext() {
        // either we have more accounts or more permissions to generate for the last account
        return accsGenerator.hasNext() || permsGenerated < permsToGenerate;
    }
    
    @Override
    public Object[] next() {
        if (permsGenerated == permsToGenerate) {
            switchAccount();
        }
        
        Object tuple[] = new Object[columnsNum];
        
        tuple[0] = currentAccId; // ap_ca_id
        tuple[1] = permACLs[permsGenerated]; // ap_acl
        tuple[2] = person.getTaxID(permCids[permsGenerated]); // ap_tax_id
        tuple[3] = person.getLastName(permCids[permsGenerated]); // ap_l_name
        tuple[4] = person.getFirstName(permCids[permsGenerated]); // ap_f_name
        
        permsGenerated++;
        
        return tuple;
    }
}
