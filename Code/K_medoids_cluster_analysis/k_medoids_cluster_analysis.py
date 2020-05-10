import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

import scipy.stats as stats
plt.rcParams['figure.figsize'] = 10, 7.5
plt.rcParams['axes.grid'] = True


from sklearn.preprocessing import StandardScaler
from sklearn import metrics
from sklearn.decomposition import PCA
import os
import sys
from sklearn import metrics

from sklearn_extra.cluster import KMedoids

sys.path.append(os.path.abspath(os.path.join('..', 'data_operations')))
import data_operations

### Read input data
KEYSPACE_NAME = 'stocks'

df=data_operations.get_input_features_from_db(tablename="input_features", KEYSPACE=KEYSPACE_NAME)

# df=pd.read_csv("input_features.csv")
# df.drop(columns=["Unnamed: 0"],inplace=True)

current_directory = os.getcwd()
slh_folder = os.path.join(current_directory, 'silhouette_plots')
elbow_folder=os.path.join(current_directory, 'elbow_plots')

if not os.path.exists(slh_folder):
    os.makedirs(slh_folder)
if not os.path.exists(elbow_folder):
    os.makedirs(elbow_folder)  


#### ELBOW METHOD AND SILHOUTTE COEFFICIENT ANALYSIS FOR OPTIMAL  CLUSTERS  FOR ALL QUARTERS

def cluster_eval(feature_df,col,quarter_list):
    s_df=pd.DataFrame()
    elbow_df=pd.DataFrame()
    
    
    for idx in range(1,len(quarter_list)):
        df = feature_df[(feature_df["start_date"] >= quarter_list[idx-1])  & (feature_df["start_date"] < quarter_list[idx])]
        
        scores = []
        distortion=[]       
        
        scalor=StandardScaler()        
        df_scaled=scalor.fit_transform(df[col])


        pc_final=PCA(n_components=len(col)).fit(df_scaled)
        reduced_cr=pc_final.fit_transform(df_scaled)


        for k in range(3,7):
            kmedoids = KMedoids(n_clusters=k, random_state=0).fit(reduced_cr)
            #km.fit(df_scaled)
            

            scores.append(metrics.silhouette_score(df_scaled, kmedoids.labels_))  
            distortion.append(round(kmedoids.inertia_,2))
       
        name="qtr"+str(idx)
        score_df=pd.DataFrame(scores,columns=[name])
        s_df=pd.concat([s_df,score_df],axis=1)
        

        dist_df=pd.DataFrame(distortion,columns=[name])
        elbow_df=pd.concat([elbow_df,dist_df],axis=1)
    return(s_df,elbow_df)  

quarter_list=["2018-04-01", "2018-07-01", "2018-10-01", "2019-01-01", "2019-04-01", "2019-07-01", "2019-10-01", "2020-01-01", "2020-04-01"] 
col=['RSI', 'ROC', 'ADOSC', 'ATR', 'EMA','Sharpe_ratio', 'average_returns','wtd_sentiment']
temp_df=df.copy()
silh_df,elbow_df=cluster_eval(temp_df,col,quarter_list)

# print(silh_df)

# print(elbow_df)

### Graphical representation of silhouette coefficient analysis of clusters quarterwise

def silh_gf(silh_df):
    k=range(3,7)
    q_list=['qtr1','qtr2','qtr3','qtr4','qtr5','qtr6','qtr7','qtr8']
    for i in q_list:
    
        plt.plot(k, silh_df[i])
        plt.xlabel('Number of clusters')
        plt.ylabel('Silhouette Coefficient')
        plt.title('Silhouette Coefficient Method For Optimal number of cluster for quarter:-'+str(i))
        filename="slhouette_plot_qtr_"+str(i)        
        plt.savefig(slh_folder+"/"+filename) 

silh_gf(silh_df)


### Graphical represntation of elbow analysis of clusters quarterwise

def elbow_gf(elbow_df):
    k=range(3,7)
    q_list=['qtr1','qtr2','qtr3','qtr4','qtr5','qtr6','qtr7','qtr8']
    for i in q_list:

        plt.plot(k, elbow_df[i], 'bx-')
        plt.xlabel('k')
        plt.ylabel('elbow')
        plt.title('Elbow Method For Optimal k for quarter:-'+str(i))
        filename="Elbow_plot_qtr_"+str(i)        
        plt.savefig(elbow_folder+"/"+filename) 

elbow_gf(elbow_df)

### K-MEDOIDS CLUSTERING 

def k_medoids(feature_df,num_clstr,quarter_list,cols,pca_folder,prof_folder,ctr_folder,cluster_folder):
    Profile=pd.DataFrame()
    company_cluster=pd.DataFrame()
           

    for idx in range(1,len(quarter_list)):
        df = feature_df.loc[(feature_df["start_date"] >= quarter_list[idx-1])  & (feature_df["start_date"] < quarter_list[idx])].reset_index(drop=True)        

        scalor=StandardScaler()
        df_scaled=scalor.fit_transform(df[cols])
        
        pc_final=PCA(n_components=len(cols)).fit(df_scaled)
        reduced_cr=pc_final.fit_transform(df_scaled)
        #km_model=KMeans(n_clusters=num_clstr,random_state=123).fit(reduced_cr)
        kmedoids = KMedoids(n_clusters=num_clstr, random_state=0).fit(reduced_cr)
        
        if(len(cols)==8):
            pca_col=['PC_0','PC_1','PC_2','PC_3','PC_4','PC_5','PC_6','PC_7']
        else:
            pca_col=['PC_0','PC_1','PC_2','PC_3','PC_4','PC_5','PC_6']
            
            
        
        pc_df=pd.DataFrame(reduced_cr,columns=pca_col)
        pc_df['symbol']=df['symbol']
        pc_df['sector']=df['sector']
        pc_df['label'] = kmedoids.labels_
        
        #save cluster results based on PCA components
        pc_df.to_csv(pca_folder+"/"+ str(idx)+"_pca_qt_result.csv")
        
        #save cluster centers quarterwise
        centroid_df=pd.DataFrame(kmedoids.cluster_centers_,columns=pca_col)
        centroid_df.to_csv(ctr_folder+"/"+ str(idx)+"_qtr_centroid.csv")
       
        
                       
        #print("Cluster for quarter",id,"\n",pd.Series.sort_index(df.label.value_counts()))

        #save orignal dataframe with labels
        df['label'] = kmedoids.labels_
        new_col=cols+['label']
        df.to_csv(cluster_folder+"/"+ str(idx)+"_qt_result.csv")


        df1=df[new_col]
        size=pd.concat([pd.Series(df1.label.size), pd.Series.sort_index(df1.label.value_counts())])
        Seg_size=pd.DataFrame(size, columns=['Seg_size'])
        Profling_output = pd.concat([df1.apply(lambda x: round(x.mean(),2)).T, df1.groupby('label').apply(lambda x: round(x.mean(),2)).T],axis=1)
        Profling_output_final=pd.concat([Seg_size.T, Profling_output], axis=0)
        
        if(num_clstr==4):
            profile_col=['Overall', 'KM_1', 'KM_2', 'KM_3','KM_4']
        if(num_clstr==3):
            profile_col=['Overall', 'KM_1', 'KM_2', 'KM_3']
        Profling_output_final.columns = profile_col
        
        #save cluster profile quarterwise
        Profling_output_final.to_csv(prof_folder+"/"+str(idx)+"_quarter_profile.csv")  
        
        

### Cluster 4 results with sentiment

current_directory = os.getcwd()
pca_dir = os.path.join(current_directory, 'Results_with_sentiment/pca_folder_cl4')
prof_dir=os.path.join(current_directory, 'Results_with_sentiment/Profiles_qtrwise_cl4')
centroid_dir=os.path.join(current_directory, 'Results_with_sentiment/cluster_centre_qtrwise_cl4')
cluster_dir=os.path.join(current_directory, 'Results_with_sentiment/cluster_df_cl4')

if not os.path.exists(pca_dir):
    os.makedirs(pca_dir)
if not os.path.exists(prof_dir):
    os.makedirs(prof_dir)    
if not os.path.exists(centroid_dir):
    os.makedirs(centroid_dir)
if not os.path.exists(cluster_dir):
    os.makedirs(cluster_dir)        

cols=['RSI', 'ROC', 'ADOSC', 'ATR', 'EMA','Sharpe_ratio', 'average_returns','wtd_sentiment']
num_clstr=4
temp=df.copy()
k_medoids(temp,num_clstr,quarter_list,cols,pca_dir,prof_dir,centroid_dir,cluster_dir)



### Cluster 4 results without sentiment 

current_directory = os.getcwd()
pca_dir = os.path.join(current_directory, 'Results_without_sentiment/pca_folder_cl4')
prof_dir=os.path.join(current_directory, 'Results_without_sentiment/Profiles_qtrwise_cl4')
centroid_dir=os.path.join(current_directory, 'Results_without_sentiment/cluster_centre_qtrwise_cl4')
cluster_dir=os.path.join(current_directory, 'Results_without_sentiment/cluster_df_cl4')

if not os.path.exists(pca_dir):
    os.makedirs(pca_dir)
if not os.path.exists(prof_dir):
    os.makedirs(prof_dir)    
if not os.path.exists(centroid_dir):
    os.makedirs(centroid_dir)
if not os.path.exists(cluster_dir):
    os.makedirs(cluster_dir)   

cols=['RSI', 'ROC', 'ADOSC', 'ATR', 'EMA','Sharpe_ratio', 'average_returns']
num_clstr=4
temp=df.copy()
k_medoids(temp,num_clstr,quarter_list,cols,pca_dir,prof_dir,centroid_dir,cluster_dir)

### Cluster 3 result with sentiment

current_directory = os.getcwd()
pca_dir = os.path.join(current_directory, 'Results_with_sentiment/pca_folder_cl3')
prof_dir=os.path.join(current_directory, 'Results_with_sentiment/Profiles_qtrwise_cl3')
centroid_dir=os.path.join(current_directory, 'Results_with_sentiment/cluster_centre_qtrwise_cl3')
cluster_dir=os.path.join(current_directory, 'Results_with_sentiment/cluster_df_cl3')

if not os.path.exists(pca_dir):
    os.makedirs(pca_dir)
if not os.path.exists(prof_dir):
    os.makedirs(prof_dir)    
if not os.path.exists(centroid_dir):
    os.makedirs(centroid_dir)
if not os.path.exists(cluster_dir):
    os.makedirs(cluster_dir)   

cols=['RSI', 'ROC', 'ADOSC', 'ATR', 'EMA','Sharpe_ratio', 'average_returns','wtd_sentiment']
num_clstr=3
temp=df.copy()
k_medoids(temp,num_clstr,quarter_list,cols,pca_dir,prof_dir,centroid_dir,cluster_dir)

#### cluster 3 results without sentiment

current_directory = os.getcwd()
pca_dir = os.path.join(current_directory, 'Results_without_sentiment/pca_folder_cl3')
prof_dir=os.path.join(current_directory, 'Results_without_sentiment/Profiles_qtrwise_cl3')
centroid_dir=os.path.join(current_directory, 'Results_without_sentiment/cluster_centre_qtrwise_cl3')
cluster_dir=os.path.join(current_directory, 'Results_without_sentiment/cluster_df_cl3')

if not os.path.exists(pca_dir):
    os.makedirs(pca_dir)
if not os.path.exists(prof_dir):
    os.makedirs(prof_dir)    
if not os.path.exists(centroid_dir):
    os.makedirs(centroid_dir)
if not os.path.exists(cluster_dir):
    os.makedirs(cluster_dir)   

cols=['RSI', 'ROC', 'ADOSC', 'ATR', 'EMA','Sharpe_ratio', 'average_returns']
num_clstr=3
temp=df.copy()
k_medoids(temp,num_clstr,quarter_list,cols,pca_dir,prof_dir,centroid_dir,cluster_dir)



