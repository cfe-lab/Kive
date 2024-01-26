#! /usr/bin/env bash

name=$1
ip=$2

echo -e "${ip}\t${name}" >> /etc/hosts
