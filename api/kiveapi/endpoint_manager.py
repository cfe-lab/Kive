class EndpointManager(object):
    def __init__(self, session):
        self.session = session

    def __getattr__(self, name):
        return SessionContext(self.session, name)


class SessionContext(object):
    def __init__(self, session, name):
        self.session = session
        self.prefix = '/api/{}/'.format(name)

    def adjust_args(self, args):
        new_args = list(args)
        if new_args:
            url = str(new_args.pop(0))
        else:
            url = ''
        new_args.insert(0, self.prefix + url)
        return new_args

    def get(self, *args, **kwargs):
        return self.session.get(*(self.adjust_args(args)), **kwargs).json()

    def post(self, *args, **kwargs):
        return self.session.post(*(self.adjust_args(args)), **kwargs).json()

    def filter(self, *args, **kwargs):
        return self.session.filter(self.prefix, *args, **kwargs).json()
