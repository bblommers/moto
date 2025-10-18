#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Get ReservedInstanceOfferings from AWS
Stores result in moto/ec2/resources/reserved_instance_offerings/{region}.json

"""

import boto3
import json
import os
import subprocess
from boto3 import Session
from time import sleep

PATH = "moto/ec2/resources/reserved_instance_offerings"


def main():
    print("Getting InstanceTypeOfferings from all regions")
    regions = []
    regions.extend(Session().get_available_regions("ec2"))
    regions.extend(Session().get_available_regions("ec2", partition_name="aws-us-gov"))
    regions.extend(Session().get_available_regions("ec2", partition_name="aws-cn"))
    print("Found " + str(len(regions)) + " regions")

    regions = ["us-east-1"]

    root_dir = (
        subprocess.check_output(["git", "rev-parse", "--show-toplevel"])
        .decode()
        .strip()
    )
    from pprint import pprint
    for region in regions:
        ec2 = boto3.client("ec2", region_name=region)
        dest = os.path.join(
            root_dir, f"{PATH}/{region}.json"
        )
        try:
            instances = []
            offerings = ec2.describe_reserved_instances_offerings()
            pprint(offerings)
            instances.extend(offerings["ReservedInstancesOfferings"])
            next_token = offerings.get("NextToken", "")
            while next_token:
                offerings = ec2.describe_reserved_instances_offerings(NextToken=next_token)
                pprint(offerings)
                instances.extend(offerings["ReservedInstancesOfferings"])
                next_token = offerings.get("NextToken", None)
                print(f"Retrieved {len(instances)} instances so far..")

            print("Writing data to {0}".format(dest))
            with open(dest, "w+") as open_file:
                json.dump(instances, open_file, indent=1)
        except Exception as e:
            print("Unable to write data to {0}".format(dest))
            print(e)
        # We don't want it to look like we're DDOS'ing AWS
        sleep(1)


if __name__ == "__main__":
    main()
