from .dictable import Dictable


class Identity(Dictable):
    def __init__(self, user, company, role, user_name=None, company_name=None):
        self._user = user
        self._company = company
        self._role = role
        self._user_name = user_name
        self._company_name = company_name

    @property
    def user(self):
        return self._user

    @property
    def company(self):
        return self._company

    @property
    def role(self):
        return self._role

    @property
    def user_name(self):
        return self._user_name

    @property
    def company_name(self):
        return self._company_name
