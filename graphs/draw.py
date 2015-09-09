#!/usr/bin/env python
import sys
import matplotlib.pyplot as plt 
import numpy as np
def time_pred(in_path,out_path):
    fig = plt.figure()
    fig.set_size_inches(8,4.8)
    ax = fig.add_subplot(111)
    res1 = []
    res2 =[]
    res3 = []
    res4 = []
    for line in open(in_path):
        fields = line.rstrip().split('\t')
        fields = [float(x) for x in fields]
        res1.append(fields[0])
        res2.append(fields[1])
        res3.append(fields[2])
        res4.append(fields[3])
    x = np.arange(0,1.1,0.1)
    print x
    ax.plot(x,res1,color='#FF6600',linestyle='-',marker='^',label='COLD')
    ax.plot(x,res2,color='#99CC00',linestyle='-.',marker='o',label='COLD,nolink')
    ax.plot(x,res3,color='#CC99FF',linestyle='-',marker='s',label='EUTB')
    ax.plot(x,res4,color='#99CCFF',linestyle='-.',marker='o',label='Pipeline')
    ax.legend(loc=4)
    ax.set_xlabel("Date Tolerance (% of #time-slice)",fontsize=16)
    ax.set_ylabel("Prediction Accuracy",fontsize=16)
    #plt.show()
    plt.savefig(out_path)
def tuple_num(out_path):
    fig = plt.figure()
    #fig.set_size_inches(8,4.8)
    ax = fig.add_subplot(111)
    res1 = [931.671451471, 12906.279152, 4123.76367542]
    res2 = [922.39838699, 13726.6611146, 4350.42673235]
   
  #     \#topic ($K$) & 50 & 100 & 150 \\ \hline %\hline
  # TI & 0.7476 & 0.7505  & 0.7349 \\ \hline%\cline{2-4}
  # WTM & \multicolumn{3}{c}{0.7705} \\ \hline%\cline{2-4}
  # COLD(C=100) & 0.8283 & {\bf 0.8397} & 0.8254 \\
  # \hline
    x = [0.5, 1.0, 1.5]
    ax.bar( [i-0.1 for i in x] ,res1,width=0.1,label='aLRU',hatch='\\',color='y')
    ax.bar( [i for i in x],res2,width=0.1,label='timestamps',hatch='/',color='c')
    ax.set_ylabel("Tuples per 100M",fontsize=16, weight='bold')
    #ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))
    ax.legend(loc='upper center', bbox_to_anchor=(0.5, 1.05),ncol=3)
    ax.set_xlim([0.2,1.8])
    ax.set_ylim([0,15000])
    ax.set_xticklabels(['YCSB','VOTER','TPC-C'],fontsize=16)
    ax.set_xlabel("Workload",fontsize=16, weight='bold')
    ax.set_xticks([0.5,1,1.5])
    #plt.show()
    plt.savefig(out_path)

def anti_cache(out_path):
    fig = plt.figure()
    #fig.set_size_inches(8,4.8)
    ax = fig.add_subplot(111)
    res1 = [45000]
    res2 = [32000]
   
  #     \#topic ($K$) & 50 & 100 & 150 \\ \hline %\hline
  # TI & 0.7476 & 0.7505  & 0.7349 \\ \hline%\cline{2-4}
  # WTM & \multicolumn{3}{c}{0.7705} \\ \hline%\cline{2-4}
  # COLD(C=100) & 0.8283 & {\bf 0.8397} & 0.8254 \\
  # \hline
    x = [0.5, 1.0, 1.5]
    ax.bar( [0.45] ,res1,width=0.1,label='H-Store',hatch='\\',color='#228B22')
    ax.bar( [0.95],res2,width=0.1,label='anti-caching',hatch='/',color='#FF6600')
    ax.set_ylabel("Transactions per second",fontsize=16, weight='bold')
    #ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))
    ax.legend(loc='upper center', bbox_to_anchor=(0.5, 1.05),ncol=3)
    ax.set_xlim([0.2,1.3])
    ax.set_ylim([0,50000])
    ax.set_xticklabels(['data=memory','data=2Xmemory'],fontsize=16)
    #ax.set_xlabel("Workload",fontsize=16, weight='bold')
    ax.set_xticks([0.5,1])
    #plt.show()
    plt.savefig(out_path)

def ext_perplexity_perf(out_path):
    fig = plt.figure()
    #fig.set_size_inches(8,4.8)
    ax = fig.add_subplot(111)
    res1 = [644455, 2036, 1962511, 4273859]
    res2 =[1492659, 613451, 4050569, 4575004]
    res3 = [1226661 ,  2035, 2521207, 4837716]
    res4 = [2078536 ,  571241, 4633451, 5204411]
   
#   \#topic ($K$) & 50 & 100 & 150 \\ \hline %\hline
#   PMTLM & 9889.48 & 8966.57 & 8483.49 \\ %\hline
#   EUTB & 4932.97 & 4778.50 & 4619.07 \\ %\hline
#   COLD(C=100) & 5200.46 & {\bf 4350.95} & 4394.46 \\
  
    x = [0.5,1,1.5,2]
    ax.bar( [i-0.2 for i in x] ,res1,width=0.1,label='lru-read',hatch='\\',color='b')
    ax.bar( [i-0.1 for i in x],res2,width=0.1,label='ts-read',hatch='/',color='r')
    ax.bar( [i+0.00 for i in x] ,res3,width=0.1,label='lru-write',hatch='|',color='g')
    ax.bar( [i+0.1 for i in x] ,res4,width=0.1,label='ts-write',hatch='|',color='m')
    ax.set_ylabel("KBytes",fontsize=10)
    #ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))
    ax.legend(loc='upper center', bbox_to_anchor=(0.5, 1.05),ncol=4)
    ax.set_xlim([0.2,2.3])
    ax.set_ylim([0,6000000])
    ax.set_xticklabels(['S0.8','S1.0','S1.1', 'S1.2'],fontsize=16)
    ax.set_xlabel("Skew factor",fontsize=10)
    ax.set_xticks([0.5,1,1.5,2])
    #plt.show()
    plt.savefig(out_path)

def link_predict_perf(out_path):
    fig = plt.figure()
    #fig.set_size_inches(8,4.8)
    ax = fig.add_subplot(111)
    res1 = [0.6105 , 0.7098  , 0.6840]
    res2 =[0.8398 , 0.8469 , 0.8425 ]
    res3 = [0.8507 , 0.8647, 0.8485]
   
  # \#community ($C$) & 50 & 100 & 150 \\ \hline %\hline
  # MMSB & 0.6105 & 0.7098  & 0.6840 \\ %\hline
  # PMTLM & 0.8398 & 0.8469 & 0.8425 \\ %\hline
  # COLD(K=100) & 0.8507 & {\bf 0.8647} & 0.8485 \\
  
    x = [0.5,1,1.5]
    ax.bar( [i-0.15 for i in x] ,res1,width=0.1,label='MMSB',hatch='\\',color='#FF6600')
    ax.bar( [i-0.05 for i in x],res2,width=0.1,label='PMTLM',hatch='/',color='#99CC00')
    ax.bar( [i+0.05 for i in x] ,res3,width=0.1,label='COLD(K=100)',hatch='|',color='#CC99FF')
    ax.set_ylabel("Link Prediction (AUC)",fontsize=16)
    #ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))
    ax.legend(loc='upper center', bbox_to_anchor=(0.5, 1.05),ncol=3)
    ax.set_xlim([0.2,1.7])
    ax.set_ylim([0.5,0.9])
    ax.set_yticklabels(['0.5','','0.6','','0.7','', '0.8','', '0.9'],fontsize=16)
    ax.set_xticks([0.5,0.6,0.7,0.8,0.9])
    ax.set_xticklabels(['50','100','150'],fontsize=16)
    ax.set_xlabel("#Community C",fontsize=16)
    ax.set_xticks([0.5,1,1.5])
    #plt.show()
    plt.savefig(out_path)

def skew4(out_path):
    fig = plt.figure()
    #fig.set_size_inches(8,4.8)
    ax = fig.add_subplot(111)
    res1 = [2618.45, 17978.96, 11843.0155556, 25602.2333333]
    res2 =[6123.74, 28654.0766667, 13244.77, 32371.2922222]

    x = [0.5,1,1.5,2]
    ax.bar( [i-0.1 for i in x], res1,width=0.1,label='lru',hatch='/',color='#99CC00')
    ax.bar( [i+0.00 for i in x] ,res2,width=0.1,label='timestamps',hatch='|',color='#CC99FF')
    #ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))
    ax.legend(loc='upper center', bbox_to_anchor=(0.5, 1.05),ncol=2)
    ax.set_xlim([0.2,2.3])
    ax.set_ylim([0,35000])
    ax.set_xticklabels(['S0.8','S1.0','S1.1','S1.2'],fontsize=16)
    ax.set_xticks([0.5,1,1.5,2])
    ax.set_xlabel("Skew Factor",fontsize=16)
    ax.set_ylabel("Transactions / second",fontsize=16)
    #plt.show()
    plt.savefig(out_path)

def training_time(out_path):
    fig = plt.figure()
    #fig.set_size_inches(8,4.8)
    ax = fig.add_subplot(111)
    res1 = [42.38, 39.12, 167.25 , 97.8525,  13.29608]
    methods = ('PMTLM', 'EUTB' ,'WTM' , 'COLD' , 'COLD(8)')
    y_pos = [1,2,3,4,5]
     #     Methods & PMTLM & EUTB & WTM & COLD & COLD (8) \\ \hline %\hline
  # Time (h) & 42.38 & 39.12 & 167.25 & 129.84 & 11.29
    ax.barh([i-0.25 for i in y_pos] ,res1,height = 0.5, hatch='/',color='#99CC00')
    #ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))
    ax.set_yticklabels(methods,fontsize=16)
    ax.set_yticks(y_pos)
    ax.set_xlabel("Time(s)",fontsize=16)
    ax.set_ylabel("Methods",fontsize=16)
    #plt.show()
    plt.savefig(out_path)
 

def param_perplexity(in_path,out_path):
    fig = plt.figure()
    fig.set_size_inches(8,4.5)
    ax=fig.add_subplot(111)
    res1 = []
    res2 =[]
    res3 = []
    res4 = []
    for line in open(in_path):
        fields = line.rstrip().split('\t')
        fields = [float(x) for x in fields]
        res1.append(fields[0])
        res2.append(fields[1])
        res3.append(fields[2])
        res4.append(fields[3])
    x = [0.5,1,1.5,2]
    ax.bar( [i-0.2 for i in x] ,res1,width=0.1,label='K=20',hatch='\\',color='#FF6600')
    ax.bar( [i-0.1 for i in x],res2,width=0.1,label='K=50',hatch='/',color='#99CC00')
    ax.bar( [i for i in x] ,res3,width=0.1,label='K=100',hatch='|',color='#CC99FF')
    ax.bar( [i+0.1 for i in x] ,res4,width=0.1,label='K=150',hatch='-',color='#99CCFF')
    ax.set_ylabel("Perplexity",fontsize=16)
    #ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))
    ax.legend(loc='upper center', bbox_to_anchor=(0.5, 1.05),ncol=4)
    ax.set_xlim([0.2,2.3])
    ax.set_ylim([0,8600])
    ax.set_xticklabels(['C=20','C=50','C=100','C=150'],fontsize=16)
    ax.set_xticks([0.5,1,1.5,2])
    #ax.bar(res3)
    #ax.bar(res4)
    #plt.show()
    plt.savefig(out_path)
def param_auc(in_path,out_path):
    fig = plt.figure()
    fig.set_size_inches(8,4.5)
    ax=fig.add_subplot(111)
    res1 = [0.804299893,0.842444446,0.849645276,0.8349456]
    res2=[0.837819382,0.848671287,0.859869915,0.84696871]
    res3=[0.849837227,0.859731227,0.864741202,0.856049581]
    res4=[0.845544536,0.838487469,0.844473523,0.830551247]
    # for line in open(in_path):
    #     fields = line.rstrip().split('\t')
    #     fields = [float(x) for x in fields]
    #     res1.append(fields[0])
    #     res2.append(fields[1])
    #     res3.append(fields[2])
    #     res4.append(fields[3])
    x = [0.5,1,1.5,2]
    ax.bar( [i-0.2 for i in x] ,res1,width=0.1,label='C=20',hatch='\\',color='#FF6600')
    ax.bar( [i-0.1 for i in x],res2,width=0.1,label='C=50',hatch='/',color='#99CC00')
    ax.bar( [i for i in x] ,res3,width=0.1,label='C=100',hatch='|',color='#CC99FF')
    ax.bar( [i+0.1 for i in x] ,res4,width=0.1,label='C=150',hatch='-',color='#99CCFF')
    ax.set_ylabel("AUC",fontsize=16)
    #ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))
    ax.legend(loc='upper center', bbox_to_anchor=(0.5, 1.05),ncol=4)
    ax.set_xlim([0.2,2.3])
    ax.set_ylim([0.7,0.9])
    ax.set_xticklabels(['K=20','K=50','K=100','K=150'],fontsize=16)
    ax.set_xticks([0.5,1,1.5,2])
    #ax.bar(res3)
    #ax.bar(res4)
    #plt.show()
    plt.savefig(out_path)
def param_rt_auc(out_path):
    fig = plt.figure()
    fig.set_size_inches(8,4.5)
    ax=fig.add_subplot(111)
    res1 = [0.816569569,0.818395692,0.816361342,0.785220436]
    res2=[0.817803569,0.820770394,0.827362933,0.805384206]
    res3=[0.821360782,0.828361933,0.839770432,0.825420544]
    res4=[0.80472904,0.822333338,0.825420544,0.809383506]
    x = [0.5,1,1.5,2]
    ax.bar( [i-0.2 for i in x] ,res1,width=0.1,label='K=20',hatch='\\',color='#FF6600')
    ax.bar( [i-0.1 for i in x],res2,width=0.1,label='K=50',hatch='/',color='#99CC00')
    ax.bar( [i for i in x] ,res3,width=0.1,label='K=100',hatch='|',color='#CC99FF')
    ax.bar( [i+0.1 for i in x] ,res4,width=0.1,label='K=150',hatch='-',color='#99CCFF')
    ax.set_ylabel("AUC",fontsize=16)
    #ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))
    ax.legend(loc='upper center', bbox_to_anchor=(0.5, 1.05),ncol=4)
    ax.set_xlim([0.2,2.3])
    ax.set_ylim([0.7,0.9])
    ax.set_xticklabels(['C=20','C=50','C=100','C=150'],fontsize=16)
    ax.set_xticks([0.5,1,1.5,2])
    #ax.bar(res3)
    #ax.bar(res4)
    #plt.show()
    plt.savefig(out_path)
def time_lag(in_path,out_path):
    fig = plt.figure()
    fig.set_size_inches(8,4.8)
    ax = fig.add_subplot(111)
    x = []
    res1 = []
    res2 =[]
    input = open(in_path)
    lines = input.readlines()
    x = [int(i) for i in lines[0].split('\t')]
    res1 = [float(i) for i in lines[1].split('\t')]
    res2 = [float(i) for i in lines[2].split('\t')]
    ax.plot(x,res1,color='#FF6600',linestyle='-',marker='o',label='High-interested Comm')
    ax.plot(x,res2,color='#3366FF',linestyle='-',marker='^',label='Medium-interested Comm')
    #ax.legend(loc='upper right')
    ax.set_yticks([0,0.002,0.004,0.006,0.008,0.01,0.012])
    ax.set_yticklabels(['0.000','0.002','0.004','0.006','0.008','0.010','0.012'])
    #ax.legend(loc='upper left')
    ax.legend(loc='upper center', bbox_to_anchor=(0.5, 1.05),ncol=2)
    ax.set_xlabel("Time Relative to Peak(Hour)",fontsize=16)
    ax.set_ylabel("Popularity",fontsize=16)
    #plt.show()
    plt.savefig(out_path)
def correlate(in_path,out_fig):
    fig = plt.figure()
    #fig.set_size_inches(8,5)
    ax = fig.add_subplot(111)
    x= []
    res = []
    import math
    for line in open(in_path):
        fields = line.rstrip().split('\t')
        x.append( math.log(float(fields[0]),10) )
        res.append(float(fields[1]))
    ax.scatter(x,res,color='#FF6600',marker='+')
    ax.set_xlabel("Community level Topic Interest",fontsize=18)
    ax.set_ylabel("Temporal Variance($10^{-5}$)",fontsize=18)
    ax.set_yticks([0,5e-6,10e-6,15e-6,20e-6,25e-6,30e-6])
    ax.set_yticklabels(['0','0.5','1','1.5','2','2.5','3'],fontsize=18)
    ax.set_ylim([0,2e-5])
    ax.set_xlim([-6,0])
    ax.set_xticks([-6,-5,-4,-3,-2,-1,0])
    ax.set_xticklabels(['$10^{-6}$','$10^{-5}$','$10^{-4}$','$10^{-3}$','$10^{-2}$','$10^{-1}$','1'],fontsize=18)
    #plt.show()
    plt.savefig(out_fig)
def correlate_cdf(in_path,out_fig):
    fig = plt.figure()
    #fig.set_size_inches(8,5)
    ax = fig.add_subplot(111)
    x= []
    res = []
    import math
    for line in open(in_path):
        fields = line.rstrip().split('\t')
        x.append( math.log(float(fields[0]),10) )
        res.append(float(fields[1]))
    ax.scatter(x,res,color='#FF6600',marker='+')
    y = res
    res_sum =  sum(res)
    y = [i/res_sum for i in res]
    cy = np.cumsum(y)
    ax.set_xlabel("Community level Topic Interest",fontsize=16)
    ax.set_ylabel("Temporal Variance ($10^{-5}$)",fontsize=16)
    ax.set_yticks([0,5e-6,10e-6,15e-6,20e-6,25e-6,30e-6])
    ax.set_yticklabels(['0','0.5','1','1.5','2','2.5','3'],fontsize=16)    #ax.set_ylim([0,2e-5])
    ax.set_ylim([0,2e-5])
    ax.set_xlim([-6,0])
    ax.set_xticks([-6,-5,-4,-3,-2,-1,0])
    ax.set_xticklabels(['$10^{-6}$','$10^{-5}$','$10^{-4}$','$10^{-3}$','$10^{-2}$','$10^{-1}$','1'],fontsize=18)
    ax2 = ax.twinx()
    ax2.plot(x,cy)
    ax2.set_ylim([0,1])
    ax2.set_ylabel("Cumulative Distribution Function", fontsize=16)
    plt.show()
    #plt.savefig(out_fig)

def speedup():
    fig = plt.figure()
    fig.set_size_inches(7,7)
    ax = fig.add_subplot(111)
    raw= [7.4795,17.9182,25.8796]
    ax.bar([i-0.2 for i in [1,2,3]],raw,width=0.4,hatch='\\',color='#FF6600')
    ax.set_xlabel("Data Size (#user)",fontsize=20)
    ax.set_ylabel("Time Cost (Hour)",fontsize=20)
    ax.set_xticks([1,2,3,])
    ax.set_yticks([0,5,10,15,20,25,30])
    ax.set_yticklabels(['0','5','10','15','20','25','30'],fontsize=20)
    ax.set_xticklabels(['0.18M','0.35M','0.52M'],fontsize=20)
    #plt.show()
    plt.savefig("./speedup_raw.pdf")
def speedup2():
    fig = plt.figure()
    fig.set_size_inches(7,7)
    ax = fig.add_subplot(111)
    raw= [97.8525,51.16725,25.8796,13.29608]
    ax.bar([i-0.2 for i in [1,2,3,4]],raw,width=0.4,hatch='/',color='#99CC00')
    ax.set_xlabel("GraphLab Nodes",fontsize=20)
    ax.set_ylabel("Time Cost (Hour)",fontsize=20)
    ax.set_xticks([1,2,3,4])
    ax.set_xticklabels(['1','2','4','8'],fontsize=20)
    ax.set_yticks([0,40,80,120])
    ax.set_yticklabels(['0','40','80','120'],fontsize=20)
    plt.savefig("./speedup_pal.pdf")
if __name__ == "__main__":
    #in_path = sys.argv[1]
    #out_fig = sys.argv[2]
    #time_pred("./time-predict.txt","./time-predict.pdf")
    #param_perplexity('./param_per.txt','./param_per.pdf')
    #param_auc('./param_auc.txt','./ycsb-IO.pdf')
    #param_rt_auc('./param_rt_auc_rtnetwork.pdf')
    #rt_pred_time("./rt_pred_time_new.txt","./rt-pred-time.pdf")
    #diff_pred_perf("./ycsb-throughput.pdf")
    #training_time("./training_time.pdf")
    #ext_perplexity_perf("./ycsb-IO-new.pdf")
    #skew4("ycsb-new.pdf")
    #tuple_num("tuples.pdf")
    anti_cache("anti-cache.pdf")

    #link_predict_perf("./link_predict_perf.pdf")
    #time_lag("./time_lag.txt","./time_lag.pdf")
    #time_lag_fit("./time_lag.txt","./time_lag_fit.pdf")
    #correlate("./corr.txt","./corr.pdf")
    #correlate_cdf("./corr.txt","./corr_cdf.pdf")

    #speedup()
    #speedup2()
