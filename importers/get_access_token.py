import os
import sys
import inspect

currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir) 

import argparse
import sys
from typing import List

from auth_helper.dummy_oauth_infrastructure.dummy_oauth import NoAuth


def parse_args(argv: List[str]):
  parser = argparse.ArgumentParser(description='Retrieve an access token')
  parser.add_argument(
    '--scopes', action='store', dest='scopes', type=str,
    help='The scope or scopes to request.  Multiple scopes should be space-separated (so, included in quotes on the command line).')
  parser.add_argument(
    '--audience', action='store', dest='audience', type=str,
    help='The audience to request.')
  return parser.parse_args(argv)


def get_access_token(scopes: str, audience: str):
  adapter = NoAuth()
  return adapter.issue_token(audience, scopes.split(' '))


if __name__ == '__main__':
  args = parse_args(sys.argv[1:])
  print(get_access_token(args.scopes, args.audience))
