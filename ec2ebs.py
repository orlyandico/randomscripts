#!/usr/bin/env python3
# coding: utf-8

import boto3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import List, Dict, Any
import logging

class AWSMetricsCollector:
    """Class to collect and analyze EC2 and EBS metrics from AWS."""
    
    def __init__(self, region: str = None):
        """Initialize AWS clients and configure logging."""
        self.session = boto3.session.Session(region_name=region)
        self.ec2_client = self.session.client('ec2')
        self.cloudwatch = self.session.client('cloudwatch')
        self.region = self.session.region_name
        
        # Configure pandas display options
        pd.set_option("display.max_columns", None)
        pd.set_option("display.max_colwidth", None)
        
        # Set up logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

    def get_ec2_instances(self) -> pd.DataFrame:
        """Retrieve EC2 instance information."""
        try:
            response = self.ec2_client.describe_instances()
            instances = []
            
            for reservation in response['Reservations']:
                instances.extend(reservation['Instances'])
            
            df = pd.DataFrame(instances)
            
            if df.empty:
                self.logger.info("No EC2 instances found in region %s", self.region)
                return pd.DataFrame()
            
            columns = [
                'ImageId', 'InstanceId', 'InstanceType', 'PrivateDnsName',
                'PrivateIpAddress', 'PublicDnsName', 'PublicIpAddress'
            ]
            return df[columns].fillna('')
            
        except Exception as e:
            self.logger.error("Error retrieving EC2 instances: %s", str(e))
            raise

    def get_ebs_volumes(self) -> pd.DataFrame:
        """Retrieve EBS volume information."""
        try:
            response = self.ec2_client.describe_volumes()
            df = pd.DataFrame(response['Volumes'])
            return df.fillna(0)
        except Exception as e:
            self.logger.error("Error retrieving EBS volumes: %s", str(e))
            raise

    def get_cloudwatch_metrics(
        self,
        namespace: str,
        metric_name: str,
        dimension_name: str,
        dimension_values: List[str],
        period: int = 3600,
        days: int = 14
    ) -> pd.DataFrame:
        """
        Generic function to retrieve CloudWatch metrics.
        
        Args:
            namespace: AWS namespace (e.g., 'AWS/EC2', 'AWS/EBS')
            metric_name: Name of the metric to retrieve
            dimension_name: Name of the dimension (e.g., 'InstanceId', 'VolumeId')
            dimension_values: List of dimension values to query
            period: Time period in seconds for each datapoint
            days: Number of days of historical data to retrieve
        """
        df_metrics = pd.DataFrame()
        
        for value in dimension_values:
            try:
                stats = self.cloudwatch.get_metric_statistics(
                    Namespace=namespace,
                    Dimensions=[{'Name': dimension_name, 'Value': value}],
                    MetricName=metric_name,
                    StartTime=datetime.now() - timedelta(days=days),
                    EndTime=datetime.now(),
                    Period=period,
                    Statistics=['Maximum']
                )
                
                if stats['Datapoints']:
                    df_temp = pd.DataFrame(stats['Datapoints'])
                    df_temp[dimension_name] = value
                    df_metrics = pd.concat([df_metrics, df_temp], ignore_index=True)
                
            except Exception as e:
                self.logger.error(
                    "Error retrieving CloudWatch metrics for %s %s: %s",
                    dimension_name, value, str(e)
                )
                
        return df_metrics

    def process_ec2_metrics(self, df_ec2: pd.DataFrame) -> pd.DataFrame:
        """Process EC2 metrics and combine with instance information."""
        if df_ec2.empty:
            return pd.DataFrame()
            
        cpu_metrics = self.get_cloudwatch_metrics(
            namespace='AWS/EC2',
            metric_name='CPUUtilization',
            dimension_name='InstanceId',
            dimension_values=df_ec2['InstanceId'].tolist()
        )
        
        if not cpu_metrics.empty:
            df_agg = (cpu_metrics.groupby("InstanceId")
                     .Maximum.agg(["mean", "std", "max", "count"])
                     .reset_index())
            df_agg.columns = ["InstanceId", "meanCPU", "stdCPU", "maxCPU", "countCPU"]
            
            return pd.merge(
                df_ec2, df_agg,
                left_on='InstanceId',
                right_on='InstanceId',
                how='left'
            ).fillna(0)
        
        return df_ec2

    def process_ebs_metrics(self, df_volumes: pd.DataFrame) -> pd.DataFrame:
        """Process EBS metrics and combine with volume information."""
        if df_volumes.empty:
            return pd.DataFrame()
            
        volume_ids = df_volumes['VolumeId'].tolist()
        
        # Get read and write IOPS
        read_ops = self.get_cloudwatch_metrics(
            namespace='AWS/EBS',
            metric_name='VolumeReadOps',
            dimension_name='VolumeId',
            dimension_values=volume_ids
        )
        
        write_ops = self.get_cloudwatch_metrics(
            namespace='AWS/EBS',
            metric_name='VolumeWriteOps',
            dimension_name='VolumeId',
            dimension_values=volume_ids
        )
        
        # Convert to per-second metrics
        for df in [read_ops, write_ops]:
            if not df.empty:
                df['Maximum'] = df['Maximum'] / 3600
        
        # Process read metrics
        if not read_ops.empty:
            df_read = (read_ops.groupby("VolumeId")
                      .Maximum.agg(["mean", "max", "count"])
                      .reset_index())
            df_read.columns = ["VolumeId", "meanReadIOPS", "maxReadIOPS", "countReadIOPS"]
        else:
            df_read = pd.DataFrame()
            
        # Process write metrics
        if not write_ops.empty:
            df_write = (write_ops.groupby("VolumeId")
                       .Maximum.agg(["mean", "max", "count"])
                       .reset_index())
            df_write.columns = ["VolumeId", "meanWriteIOPS", "maxWriteIOPS", "countWriteIOPS"]
        else:
            df_write = pd.DataFrame()
            
        # Combine metrics with volume information
        df_result = df_volumes.copy()
        if not df_read.empty:
            df_result = pd.merge(df_result, df_read, on='VolumeId', how='left')
        if not df_write.empty:
            df_result = pd.merge(df_result, df_write, on='VolumeId', how='left')
            
        df_result = df_result.fillna(0)
        
        # Calculate total IOPS
        if 'maxReadIOPS' in df_result.columns and 'maxWriteIOPS' in df_result.columns:
            df_result['MaxIOPS'] = df_result['maxReadIOPS'] + df_result['maxWriteIOPS']
            df_result['MeanIOPS'] = df_result['meanReadIOPS'] + df_result['meanWriteIOPS']
            
        # Extract instance IDs from attachments
        df_result['InstanceId'] = ''
        for idx, row in df_result.iterrows():
            if isinstance(row['Attachments'], list) and row['Attachments']:
                df_result.at[idx, 'InstanceId'] = row['Attachments'][0].get('InstanceId', '')
                
        # Select final columns
        final_columns = [
            'AvailabilityZone', 'CreateTime', 'Encrypted', 'Size', 'VolumeId',
            'Iops', 'VolumeType', 'Throughput', 'MeanIOPS', 'MaxIOPS', 'InstanceId'
        ]
        
        return df_result[final_columns]

    def generate_reports(self) -> None:
        """Generate and save EC2 and EBS reports."""
        try:
            self.logger.info("Retrieving EC2 instances...")
            df_ec2 = self.get_ec2_instances()
            if not df_ec2.empty:
                df_ec2_processed = self.process_ec2_metrics(df_ec2)
                df_ec2_processed.to_csv(f"ec2_report-{self.region}.csv", index=False)
                self.logger.info("EC2 report generated successfully")
            
            self.logger.info("Retrieving EBS volumes...")
            df_volumes = self.get_ebs_volumes()
            if not df_volumes.empty:
                df_volumes_processed = self.process_ebs_metrics(df_volumes)
                df_volumes_processed.to_csv(f"ebs_report-{self.region}.csv", index=False)
                self.logger.info("EBS report generated successfully")
                
        except Exception as e:
            self.logger.error("Error generating reports: %s", str(e))
            raise

def main():
    """Main function to run the AWS metrics collection."""
    try:
        collector = AWSMetricsCollector()
        collector.generate_reports()
    except Exception as e:
        logging.error("Failed to generate reports: %s", str(e))
        raise

if __name__ == "__main__":
    main()