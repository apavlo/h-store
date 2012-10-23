package edu.brown.benchmark.ycsb.distributions;


/**                                                                                                                                                                                
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.                                                                                                                             
 *                                                                                                                                                                                 
 * Licensed under the Apache License, Version 2.0 (the "License"); you                                                                                                             
 * may not use this file except in compliance with the License. You                                                                                                                
 * may obtain a copy of the License at                                                                                                                                             
 *                                                                                                                                                                                 
 * http://www.apache.org/licenses/LICENSE-2.0                                                                                                                                      
 *                                                                                                                                                                                 
 * Unless required by applicable law or agreed to in writing, software                                                                                                             
 * distributed under the License is distributed on an "AS IS" BASIS,                                                                                                               
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or                                                                                                                 
 * implied. See the License for the specific language governing                                                                                                                    
 * permissions and limitations under the License. See accompanying                                                                                                                 
 * LICENSE file.                                                                                                                                                                   
 */

import java.util.Random;

/**
 * This random number generator generates a custom skew with according to two parameters, data skew and access skew. Data skew represents what percentage of 
 * the total range of numbers is the "hot" part. Access skew represents what percentage of the numbers will be generated for the "hot" part of the range. For
 * example, an 80/20 skew means that 80% of the numbers will be generated from 20% of the total range of values. In this implementation, the hot range
 * is the range [0, data skew ceiling], i.e. the lower end of the range. 
 */

public class CustomSkewGenerator extends IntegerGenerator
{
	private Random rand; 
	
	int access_skew; 
	int data_skew; 
	int max; 
	
	// integers in the range 0 < x < data_skew_ceiling will represent the "hot" numbers getting access_skew% of the accesses 
	int data_skew_ceiling; 
	
	public CustomSkewGenerator(int _max, int _access_skew, int _data_skew)
	{
		rand = new Random(); 
				
		access_skew = _access_skew;
		max = _max;  
		
		data_skew_ceiling = (int)(max * (_data_skew / (double)100)); 
	}
	
	public int nextInt()
	{
		int key = 0; 
		int access_skew_rand = rand.nextInt(100); 
		
		if(access_skew_rand < access_skew)  // generate a number in the range 0 < x < data_skew_ceiling
		{
			key = rand.nextInt(data_skew_ceiling); 
		}
		else  // generate a number in the range data_skew_ceiling < x < max
		{
			key = rand.nextInt(max - data_skew_ceiling + 1) + data_skew_ceiling; 
		}
		
		return key; 
	}
	
	public double mean()
	{
		return 0; 
	}
}




