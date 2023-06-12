#!/usr/bin/env python3
# coding: utf-8

# In[80]:


# extract EC2 and EBS usage metrics

import boto3
import pandas as pd
import numpy as np
from datetime import date, timedelta, datetime
import json
import math
import os

client = boto3.client('ec2')
cw = boto3.client('cloudwatch')
sess = boto3.session.Session()
region = sess.region_name


pd.set_option("display.max_columns", None)
pd.set_option("display.max_colwidth", None)

print(region)


# In[110]:


# get instances in this region
response = client.describe_instances()
rs = response['Reservations']

df = pd.DataFrame()

for i in rs:
    j = i['Instances']
    df = pd.concat([df, pd.DataFrame(j) ], ignore_index=True)

df = df[ ['ImageId', 'InstanceId', 'InstanceType', 'PrivateDnsName', 'PrivateIpAddress', 'PublicDnsName', 'PublicIpAddress' ]]
df = df.fillna('')
df


# In[65]:


# get EBS volumes in this region
response = client.describe_volumes()
rs = response['Volumes']
df_v = pd.DataFrame(rs)
df_v = df_v.fillna(0)

df_v


# In[66]:


# we can only fetch 1440 data points from Cloudwatch, so over a 2-week period (20160 minutes)
# our sampling interval is 14 minutes; also note we are fetching the *MAXIMUM* over each sampling period
# however, let's use a less aggressive 1-hour sampling interval

dfutil = pd.DataFrame()

for ec2id in df['InstanceId']:

    # CPUUtilization
    stats = cw.get_metric_statistics(
        Namespace='AWS/EC2',
        Dimensions=[
            {
                'Name': 'InstanceId',
                'Value': ec2id
            }
        ],
        MetricName='CPUUtilization',
        StartTime=datetime.now() - timedelta(days=14),
        EndTime=datetime.now(),
        Period=3600,
        Statistics=[ 'Maximum' ])

    df2 = pd.DataFrame(stats['Datapoints'])
    df2['InstanceId'] = ec2id

    dfutil = pd.concat([dfutil, df2], ignore_index=True)


dfutil


# In[67]:


# for each InstanceId, get the average and maximum CPU utilization
df_agg = dfutil.groupby("InstanceId").Maximum.agg(["mean", "std", "max", "count"]).reset_index()

df_agg.columns = [ "InstanceId", "meanCPU", "stdCPU", "maxCPU", "countCPU" ]
df_agg


# In[117]:


df_ec2 = pd.merge(df, df_agg, left_on='InstanceId', right_on='InstanceId', how='left')
df_ec2 = df_ec2.fillna(0)
df_ec2


# In[118]:


# get Cloudwatch usage for volumes

dfiops_read = pd.DataFrame()
dfiops_write = pd.DataFrame()

for volid in df_v['VolumeId']:

    # VolumeReadOps
    stats = cw.get_metric_statistics(
        Namespace='AWS/EBS',
        Dimensions=[
            {
                'Name': 'VolumeId',
                'Value': volid
            }
        ],
        MetricName='VolumeReadOps',
        StartTime=datetime.now() - timedelta(days=14),
        EndTime=datetime.now(),
        Period=3600,
        Statistics=[ 'Maximum' ])

    df2 = pd.DataFrame(stats['Datapoints'])
    df2['VolumeId'] = volid

    dfiops_read = pd.concat([dfiops_read, df2], ignore_index=True)

    # VolumeWriteOps
    stats = cw.get_metric_statistics(
        Namespace='AWS/EBS',
        Dimensions=[
            {
                'Name': 'VolumeId',
                'Value': volid
            }
        ],
        MetricName='VolumeWriteOps',
        StartTime=datetime.now() - timedelta(days=14),
        EndTime=datetime.now(),
        Period=3600,
        Statistics=[ 'Maximum' ])

    df2 = pd.DataFrame(stats['Datapoints'])
    df2['VolumeId'] = volid

    dfiops_write = pd.concat([dfiops_write, df2], ignore_index=True)
    
# note that VolumeReadOps and VolumeWriteOps is specified over the period! in the above case 3600
dfiops_read['Maximum'] = dfiops_read['Maximum'] / 3600
dfiops_write['Maximum'] = dfiops_write['Maximum'] / 3600


# In[119]:


dfiops_write


# In[120]:


# for each VolumeId, get the average and maximum read/write utilization
df_agg_read = dfiops_read.groupby("VolumeId").Maximum.agg(["mean", "max", "count"]).reset_index()

df_agg_read.columns = [ "VolumeId", "meanReadIOPS", "maxReadIOPS", "countReadIOPS" ]
df_agg_read


# In[121]:


# for each VolumeId, get the average and maximum read/write utilization
df_agg_write = dfiops_write.groupby("VolumeId").Maximum.agg(["mean", "max", "count"]).reset_index()

df_agg_write.columns = [ "VolumeId", "meanWriteIOPS", "maxWriteIOPS", "countWriteIOPS" ]
df_agg_write


# In[122]:


df_v2 = pd.merge(df_v, df_agg_read, left_on='VolumeId', right_on='VolumeId', how='left')
df_v3 = pd.merge(df_v2, df_agg_write, left_on='VolumeId', right_on='VolumeId', how='left')

df_v3['MaxIOPS'] = df_v3['maxReadIOPS'] + df_v3['maxWriteIOPS']
df_v3['MeanIOPS'] = df_v3['meanReadIOPS'] + df_v3['meanWriteIOPS']
df_v3 = df_v3.fillna(0)

df_v3


# In[123]:


# extract the attachment information
df_v3['InstanceId'] = ''
for v in df_v3['Attachments']:
    for w in v:
        iid = w['InstanceId']
        vid = w['VolumeId']
    
        print(iid, vid)
        df_v3['InstanceId'] = np.where(df_v3['VolumeId'] == vid, iid, df_v3['InstanceId'])
          
df_v4 = df_v3[ ['AvailabilityZone', 'CreateTime', 'Encrypted', 'Size', 'VolumeId',
              'Iops', 'VolumeType', 'Throughput', 'MeanIOPS', 'MaxIOPS', 'InstanceId'] ]

df_v4


# In[124]:


# write out the EC2 and EBS reports
df_ec2.to_csv("ec2_report-" + region + ".csv", index=False)
df_v4.to_csv("ebs_report-" + region + ".csv", index=False)

