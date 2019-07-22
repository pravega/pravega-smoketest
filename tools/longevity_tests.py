#!/usr/bin/env python3
"""Tasks for running and stopping pravega Smoketest tests. All commands will presume helm & kubectl installed
and that kubectl is pointing at a K8s cluster
"""
from subprocess import call
import sys
import argparse
import json
import yaml

def deploy_test(test_name, image_repository, image_tag, controller_uri):
    """Runs a given test against a K8s cluster; assumes kubectl has been configured beforehand"""
    helm_overrides = "pravega_smoketest_test.test_name_kebab={},pravega_smoketest_test.num_workers={}" \
        .format(test_name, count_workers_needed(test_name))

    if (image_repository != None):
        helm_overrides += ",image.repository={}".format(image_repository)

    if (image_tag != None):
        helm_overrides += ",image.tag={}".format(image_tag)

    if (controller_uri != None):
        helm_overrides += ",pravega_smoketest_test.controller_uri={}".format(controller_uri)
 
    release_name = "pravega-smoketest-" + test_name
    call(["helm", "install", "charts/pravega-smoketest", "--name", release_name, "--namespace", "pravega-smoketest", \
        "--set", helm_overrides])

def destroy_test(test_name):
    """Removes a test from a cluster. Assumes kubectl has been configured beforehand."""

    release_name = "pravega-smoketest-" + test_name
    call(["helm", "delete", "--purge", release_name])

def count_workers_needed(test_name):
    """From a test's name, read the config file and determine how many workers to deploy for it."""
    with open('charts/pravega-smoketest/templates/pravega-smoketest-tests.yaml', 'r') as helm_config_map:
        test = json.loads(yaml.load(helm_config_map)["data"][test_name])
        tasks = test["tasks"]
        count = 0
        for task in tasks:
            count = count + task.get("duplicates", 1)
        return count

def main():
    """Main method. Start up or tear down a test"""
    parser = argparse.ArgumentParser(description="Deploy a pravega smoketest test")
    parser.add_argument("task",
                        choices=["deploy", "destroy"],
                        help="Name of task to run")
    parser.add_argument("--test",
                        default="small-scale",
                        help="Test configuration to start. Name should be in kebab case")
    parser.add_argument("--image-repository",
                        help="Image repository of pravega smoketest to use.")
    parser.add_argument("--image-tag",
                        help="Image tag of pravega smoketest to use.")
    parser.add_argument("--controller-uri",
                        help="Full URI of the Pravega controller.")
    args = parser.parse_args()

    if args.task == "deploy":
        deploy_test(args.test, args.image_repository, args.image_tag, args.controller_uri)
    if args.task == "destroy":
        destroy_test(args.test)

if __name__ == "__main__":
    sys.exit(main())
