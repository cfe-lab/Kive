import re


class EndpointManager:
    def __init__(self, session):
        self.session = session

    def __getattr__(self, name):
        return SessionContext(self.session, name)


class SessionContext:
    def __init__(self, session, name):
        self.session = session
        self.prefix = '/api/{}/'.format(name)

    def adjust_args(self, args):
        new_args = list(args)
        if new_args:
            url = str(new_args.pop(0))
            if re.match(r'^\d+$', url):
                # Make it easier to post to an id, if trailing slash required.
                url += '/'
        else:
            url = ''
        new_args.insert(0, self.prefix + url)
        return new_args

    def get(self, *args, **kwargs):
        return self.session.get(*(self.adjust_args(args)), **kwargs).json()

    def head(self, *args, **kwargs):
        return self.session.head(*(self.adjust_args(args)), **kwargs).headers

    def filter(self, *args, **kwargs):
        return self.session.filter(self.prefix, *args, **kwargs).json()

    def post(self, *args, **kwargs):
        return self.session.post(*(self.adjust_args(args)), **kwargs).json()

    def patch(self, *args, **kwargs):
        return self.session.patch(*(self.adjust_args(args)), **kwargs).json()

    def delete(self, *args, **kwargs):
        self.session.delete(*(self.adjust_args(args)), **kwargs)
