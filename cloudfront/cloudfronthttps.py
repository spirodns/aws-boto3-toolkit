#!/usr/bin/env python3

import boto3 
import pprint
import argparse
import sys

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

    return config,etag

def cloudfront_distribution_info_verbose(distribution_id):
    config,etag = cloudfront_distribution_info(distribution_id)
    default_behavior = config['DefaultCacheBehavior']
    default_behavior_viewer_protocol_policy = default_behavior['ViewerProtocolPolicy'] 
    print(f"currently has default cache behavior: ")
    pprint.pprint(f"{default_behavior}")
    print(f"and viewer protocol policy:")
    pprint.pprint(f"{default_behavior_viewer_protocol_policy}")

def change_distribution_policies(distribution_id):
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
    
    print(f"Updated distribution to redirect HTTP to HTTPS for viewers")




def main(update_policy,verbose):

    distribution_id_list = cloudfront_distributions_list()
    print(distribution_id_list)
    for distribution_id in distribution_id_list:
        print(f"Distribution: {distribution_id}")

        if verbose:
            cloudfront_distribution_info_verbose(distribution_id)
        if update_policy is None:
            user_input = input("Do you want to update the ViewerProtocolPolicy to 'redirect-to-https'? (yes/no): ").strip().lower()
            #update_policy = user_input in ('yes', 'y')
            change_policy = user_input in ('yes', 'y')
        else:
            change_policy = update_policy
    
        if change_policy:
            change_distribution_policies(distribution_id)
            print("Update complete.")
        else:
            print("No updates made.")

    
    return 0  # Success
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Tool to manage CloudFront distributions.")
    parser.add_argument('--update-policy', type=str, choices=['yes', 'no'], help="Update the ViewerProtocolPolicy to 'redirect-to-https' (yes/no).")
    parser.add_argument('-v', '--verbose', action='store_true', help="Increase output verbosity.")
    
    args = parser.parse_args()

    # Convert the --update-policy to a boolean if provided, else None
    update_policy = None
    if args.update_policy:
        update_policy = args.update_policy == 'yes'

    sys.exit(main(update_policy,args.verbose))
