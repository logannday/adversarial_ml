import pandas as pd
from pprint import pprint

def load_data(path, only_floats=True):
    df = pd.read_json(path)
    # Extract the buckets from the aggregation
    buckets = df['aggregations']['flow_features']['buckets']
    
    # Convert buckets to a list of dictionaries for DataFrame
    rows = []
    for bucket in buckets:
        row = {
            'src_ip': bucket['key']['src_ip'],
            'dest_ip': bucket['key']['dest_ip'],
            'flow_count': bucket['doc_count'],
            'total_bytes_sent': bucket['total_bytes_sent']['value'],
            'total_bytes_received': bucket['total_bytes_received']['value'],
            'total_packets_sent': bucket['total_packets_sent']['value'],
            'total_packets_received': bucket['total_packets_received']['value'],
            'connection_count': bucket['connection_count']['value'],
            'avg_flow_duration': bucket['avg_flow_duration']['value'],
            'unique_ports': bucket['unique_ports']['value']
        }
        rows.append(row)
    
    # Create DataFrame
    ip_pairs_df = pd.DataFrame(rows)
    
    # Display basic information about the DataFrame
    # print(f"Shape: {ip_pairs_df.shape}")
    # print(f"\nColumns: {ip_pairs_df.columns.tolist()}")
    # print(f"\nFirst 5 rows:")
    # print(ip_pairs_df.head())
    
    # Calculate some derived features
    ip_pairs_df['total_bytes'] = ip_pairs_df['total_bytes_sent'] + ip_pairs_df['total_bytes_received']
    ip_pairs_df['total_packets'] = ip_pairs_df['total_packets_sent'] + ip_pairs_df['total_packets_received']
    ip_pairs_df['bytes_ratio'] = ip_pairs_df['total_bytes_sent'] / (ip_pairs_df['total_bytes_received'] + 1)  # +1 to avoid division by zero
    ip_pairs_df['packets_ratio'] = ip_pairs_df['total_packets_sent'] / (ip_pairs_df['total_packets_received'] + 1)

    feature_cols = ['flow_count', 'total_bytes_sent', 'total_bytes_received', 
                'total_packets_sent', 'total_packets_received', 
                'avg_flow_duration', 'unique_ports']
    if only_floats:
        return ip_pairs_df[feature_cols]
    
    return ip_pairs_df
