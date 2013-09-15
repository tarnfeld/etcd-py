#!/usr/bin/env python

"""
etcdctl.py - a command line utility for talking to etcd

This is intended as an example of using the etcd-py package
"""

import sys
import inspect
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
    parser.add_argument("command", default=None, nargs='+',
                        help="Command to execute against the etcd server")
    args = parser.parse_args()
    return args


def arg_range(method):
    """Determines the minimum and maximum number of arguments for the method

    method: the method to check

    returns: an xrange object
    """
    arg_spec = inspect.getargspec(method)
    if arg_spec.args:
        args_count = len(arg_spec.args)
    else:
        args_count = 0
    if arg_spec.defaults:
        defaults_count = len(arg_spec.defaults)
    else:
        defaults_count = 0
    minimum = args_count - defaults_count
    maximum = args_count
    return xrange(minimum, maximum + 1)


def main():
    """Etcd command line tool"""
    args = get_args()
    client = etcd.Etcd(host=args.host, port=args.port, ssl_cert=args.cert,
                       ssl_key=args.key, follow_leader=args.follow,
                       autostart=True)
    exit_code = 0
    operation = args.command[0]
    arguments = args.command[1:]
    if operation == "set":
        if len(arguments) in arg_range(client.set):
            client.set(*arguments)
        else:
            sys.stderr.write("Incorrect number of arguments")
            exit_code = 1
    elif args.command[0] == "get":
        if len(arguments) in arg_range(client.get):
            client.get(*arguments)
        else:
            sys.stderr.write("Incorrect number of arguments")
            exit_code = 1
    elif args.command[0] == "list":
        if len(arguments) in arg_range(client.list):
            client.get(*arguments)
        else:
            sys.stderr.write("Incorrect number of arguments")
            exit_code = 1
    elif args.command[0] == "recurse":
        if len(arguments) in arg_range(client.get_recursive):
            client.get_recursive(*arguments)
        else:
            sys.stderr.write("Incorrect number of arguments")
            exit_code = 1
    elif args.command[0] == "delete":
        if len(arguments) in arg_range(client.delete):
            client.delete(*arguments)
        else:
            sys.stderr.write("Incorrect number of arguments")
            exit_code = 1
    elif args.command[0] == "watch":
        if len(arguments) in arg_range(client.watch):
            client.watch(*arguments)
        else:
            sys.stderr.write("Incorrect number of arguments")
            exit_code = 1
    elif args.command[0] == "testandset":
        if len(arguments) in arg_range(client.testandset):
            client.testandset(*arguments)
        else:
            sys.stderr.write("Incorrect number of arguments")
            exit_code = 1
    elif args.command[0] == "machines":
        if len(arguments) in arg_range(client.machines):
            client.machines(*arguments)
        else:
            sys.stderr.write("Incorrect number of arguments")
            exit_code = 1
    elif args.command[0] == "leader":
        if len(arguments) in arg_range(client.leader):
            client.leader(*arguments)
        else:
            sys.stderr.write("Incorrect number of arguments")
            exit_code = 1
    else:
        sys.stderr.write("Command must be one of set, get, list, recurse, \
delete, watch, testandset, machines, leader")
        exit_code = 1

    client.close()
    return exit_code


if __name__ == '__main__':
    sys.exit(main())
