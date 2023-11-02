#!/usr/bin/env python3

import boto3 
import pprint

# Create CloudFront client
cf_client = boto3.client('cloudfront')


def cloudfront_distributions_list():
    # Fetch the list of distributions
    distributions = cf_client.list_distributions()
    distribution_id_list = []
    # Extract and print the distribution IDs
    for distribution in distributions['DistributionList']['Items']: 
        distribution_id_list.append(distribution['Id'])

    return distribution_id_list

def cloudfront_distribution_info(distribution_id):
    # Fetch the current distribution configuration
    dist_config_response = cf_client.get_distribution_config(Id=distribution_id)
    config = dist_config_response['DistributionConfig']
    etag = dist_config_response['ETag']
    default_behavior = config['DefaultCacheBehavior']
    print(f"Distribution: {distribution_id} currently has default cache behavior: ")
    pprint.pprint(f"{config}")
    #pprint.pprint(f"{default_behavior}")

    return config,etag


def change_distribution_policies(distribution_id_list):
    for distribution_id in distribution_id_list:
        # Get the info about the distro
        config,etag = cloudfront_distribution_info(distribution_id)
        # Update default behavior to use HTTPS only
        default_behavior = config['DefaultCacheBehavior']
        default_behavior['ViewerProtocolPolicy'] = 'redirect-to-https'
        # If you have more cache behaviors, iterate and update them
        if 'CacheBehaviors' in config and 'Items' in config['CacheBehaviors']:
            for behavior in config['CacheBehaviors']['Items']:
                behavior['ViewerProtocolPolicy'] = 'redirect-to-https'
        # Update origin settings for encryption in transit
        for origin in config['Origins']['Items']:
            if 'CustomOriginConfig' in origin:
                # Ensure encryption in transit to custom origins
                if origin['CustomOriginConfig']['OriginProtocolPolicy'] != 'https-only':
                    origin['CustomOriginConfig']['OriginProtocolPolicy'] = 'match-viewer'
        

        # Update the distribution with the modified configuration
        cf_client.update_distribution(
            Id=distribution_id,
            DistributionConfig=config,
            IfMatch=etag  # This ensures you're updating the version of the config you just fetched
        )
        
        print(f"Updated distribution to redirect HTTP to HTTPS for viewers and ensured")




def main():

    distribution_id_list = cloudfront_distributions_list()
    print(distribution_id_list)

    change_distribution_policies(distribution_id_list)

    
if __name__ == "__main__":
    main()

