#!/usr/bin/env python

"""
etcdctl.py - a command line utility for talking to etcd

This is intended as an example of using the etcd-py package
"""

import sys
import argparse

import etcd


def get_args():
    """Returns a namespace containing command line arguments"""
    parser = argparse.ArgumentParser()
    parser.add_argument("-H", "--host", default="127.0.0.1",
                        help="the etcd host to connect to")
    parser.add_argument("-P", "--port", default=4001, type=int,
                        help="port for the etcd host")
    parser.add_argument("-C", "--cert", default=None,
                        help="SSL client certificate")
    parser.add_argument("-K", "--key", default=None,
                        help="SSL client key")
    parser.add_argument("-F", "--follow", default=True,
                        help="Follow the current etcd leader")
    args = parser.parse_args()
    return args


def main():
    """Etcd command line tool"""
    args = get_args()
    client = etcd.Etcd(host=args.host, port=args.port, ssl_cert=args.cert,
                       ssl_key=args.key, follow_leader=args.follow,
                       autostart=True)
    client.close()
    return 0


if __name__ == '__main__':
    sys.exit(main())
