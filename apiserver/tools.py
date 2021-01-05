""" Command line tools for the API server """
from argparse import ArgumentParser

import dpath
from humanfriendly import parse_timespan


def setup():
    from apiserver.database import db
    db.initialize()


def gen_token(args):
    from apiserver.bll.auth import AuthBLL
    resp = AuthBLL.get_token_for_user(args.user_id, args.company_id, parse_timespan(args.expiration))
    print('Token:\n%s' % resp.token)


def safe_get(obj, glob, default=None, separator="/"):
    try:
        return dpath.get(obj, glob, separator=separator)
    except KeyError:
        return default


if __name__ == '__main__':
    top_parser = ArgumentParser(__doc__)

    subparsers = top_parser.add_subparsers(title='Sections')

    token = subparsers.add_parser('token')
    token_commands = token.add_subparsers(title='Commands')
    token_create = token_commands.add_parser('generate', description='Generate a new token')
    token_create.add_argument('--user-id', '-u', help='User ID', required=True)
    token_create.add_argument('--company-id', '-c', help='Company ID', required=True)
    token_create.add_argument('--expiration', '-exp',
                              help="Token expiration (time span, shorthand suffixes are supported, default 1m)",
                              default=parse_timespan('1m'))
    token_create.set_defaults(_func=gen_token)

    args = top_parser.parse_args()
    if args._func:
        setup()
        args._func(args)
