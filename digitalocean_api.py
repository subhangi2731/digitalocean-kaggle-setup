#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'Stefan Jansen'

import argparse
import logging
import pprint
import sys
from os import environ
from os.path import expanduser

import requests
from digitalocean import Droplet, Manager
from storm.parsers.ssh_config_parser import ConfigParser as StormParser


def parse_args():
    parser = argparse.ArgumentParser(description='Launch Digital Ocean droplet and optionally download Kaggle data ' \
                                                 'for interactive analysis using Jupyter notebook')
    parser.add_argument('--user', '-u', help='Kaggle user name')
    parser.add_argument('--password', '-p', help='Kaggle password')
    parser.add_argument('--hdf', default=False, action='store_true', help='Store in HDF (requires Kaggle credentials)')
    parser.add_argument('--destroy', '-d', default=False, action='store_true', help='destroy active droplets')
    parser.add_argument('--size', '-s', default='512mb', help='droplet memory (default: 512mb).')
    parser.add_argument('--region', '-r', default='nyc2', help='DO launch region zone')
    parser.add_argument('--image', '-i', default='ubuntu-16-04-x64', help='Droplet image name')
    parser.add_argument('--name', '-n', default='kaggle-droplet', help='Droplet name')
    parser.add_argument('--authorized_key', '-k', default='id_rsa', help='ssh key for droplet user `kaggle`')
    return parser.parse_args()


def getenv(key, default=None):
    """Get an environment variable, return None if it doesn't exist.
    The optional second argument can specify an alternate default.
    key, default and the result are str."""
    return environ.get(key, default)


def confirmation(question, default='no'):
    """Ask a yes/no question via raw_input() and return their answer.
    The "answer" return value is True for "yes" or False for "no".
    :param question: str - presented to the user
    :param default: str - 'yes', 'no', or None
    :return: bool
    """
    valid = dict(yes=True, y=True, ye=True, no=False, n=False)
    if default is None:
        prompt = ' [y/n] '
    elif default == 'yes':
        prompt = ' [Y/n] '
    elif default == 'no':
        prompt = ' [y/N] '
    else:
        raise ValueError("invalid default answer: '{}'".format(default))

    while True:
        sys.stdout.write(question + prompt)
        choice = input().lower()
        if default is not None and choice == '':
            return valid[default]
        elif choice in valid:
            return valid[choice]
        else:
            sys.stdout.write("Please respond with 'yes' or 'no' (or 'y' or 'n').\n")


class DigitalOcean:
    """Automate Digital Ocean droplet deployment & management"""

    def __init__(self, api_token):
        self.api_token = api_token
        self.ssh_id = self.get_ssh_keys()
        self.ssh_config = StormParser(expanduser('~/.ssh/config'))
        self.droplet = {}
        self.droplet_attrs = ['name', 'disk', 'image', 'size_slug', 'ip_address']

    def update_ssh(self):
        """Update ssh config with new ip address and add missing users"""

        self.ssh_config.load()
        users = {'do': 'root', 'do2': 'kaggle'}
        for host in users.keys():
            if self.ssh_config.search_host(host):
                self.ssh_config.update_host(host, {'user': users[host], 'hostname': self.droplet['ip_address']})
            else:
                self.ssh_config.add_host(host, {'user': users[host], 'hostname': self.droplet['ip_address']})
        self.ssh_config.write_to_ssh_config()

    def update_config_script(self, user=None, password=None, authorized_key='id_rsa', hdf=False):
        """Make requisite substitutions in base cloud-config file"""

        with open('cloud-config.txt') as txt:
            user_data = txt.read()

        if user and password:
            user_data = user_data.replace('KAGGLE_CREDENTIALS',
                                          '  - export KAGGLE_USER={0}\n  - export KAGGLE_PASSWD={1}'.format(user,
                                                                                                            password))
            download_instructions = open('download_instructions.txt').read()
            if hdf:
                download_instructions += "\n  - python3 -c 'from kaggle_data import data_to_hdf; data_to_hdf()'"
            user_data = user_data.replace('DATA_DOWNLOAD', download_instructions)

        else:
            user_data = user_data.replace('KAGGLE_CREDENTIALS', '')
            user_data = user_data.replace('DATA_DOWNLOAD', '')

        if authorized_key:
            user_data = user_data.replace('SSH_AUTHORIZED_KEYS', 'ssh-authorized-keys:\n    - {}'.format(
                open(expanduser('~/.ssh/{}.pub'.format(authorized_key))).read()))
        else:
            user_data.replace('SSH_AUTHORIZED_KEYS', '')
        return user_data

    def launch(self, name='kaggle-droplet', region='nyc2', image='ubuntu-16-04-x64', size='512mb', user=None,
               password=None, authorized_key='id_rsa', hdf=False):
        """Launch DigitalOcean droplet instance"""

        user_data = self.update_config_script(user=user, password=password, authorized_key=authorized_key, hdf=hdf)

        droplet = Droplet(token=self.api_token, name=name, region=region, image=image, size_slug=size, backups=False,
                          ssh_keys=self.ssh_id, user_data=user_data)
        droplet.create()

        self.droplet['id'] = droplet.id
        while not self.droplet.get('ip_address', None):
            self.droplet['ip_address'] = droplet.load().ip_address
        self.update_ssh()
        pprint.pprint(('Name: {}'.format(droplet.name), 'Image: {}'.format(droplet.image.get('slug')),
                       'Memory: {}'.format(droplet.size_slug), 'Disk Size: {}'.format(droplet.disk),
                       'IP Address: {}'.format(droplet.ip_address)))

    def get_droplets(self):
        """Get active droplets"""
        manager = Manager(token=self.api_token)
        my_droplets = manager.get_all_droplets()
        return my_droplets

    def destroy(self):
        """Destroy all active droplets"""
        my_droplets = self.get_droplets()
        for droplet in my_droplets:
            droplet.destroy()

    def get_ssh_keys(self):
        """Get ssh keys stored with DigitalOcean"""

        do_ssh_url = 'https://api.digitalocean.com/v2/account/keys'
        headers = dict(Authorization='Bearer {}'.format(self.api_token))
        response = requests.get(url=do_ssh_url, headers=headers)
        ssh_keys = []
        for ssh_key in response.json().get('ssh_keys'):
            ssh_keys.append(ssh_key.get('id'))
        return ssh_keys


def main():
    logging.basicConfig()
    args = parse_args()
    api_token = getenv('DO_API_TOKEN')
    if not api_token:
        print('Please run `export DO_API_KEY=api_token` to set Digital Ocean API key environment variable. Exiting.')
        sys.exit(1)
    if not args.user:
        print('No kaggle user provided, omitting download.')
    do = DigitalOcean(api_token)

    kwargs = vars(args)
    if kwargs.pop('destroy'):
        if confirmation('Are you sure you want to destroy all active droplets?'):
            do.destroy()
    do.launch(**kwargs)


if __name__ == '__main__':
    main()
