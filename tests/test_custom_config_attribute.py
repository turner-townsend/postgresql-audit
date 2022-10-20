# -*- coding: utf-8 -*-
import sqlalchemy as sa
import pytest

from postgresql_audit import VersioningManager

from .utils import last_activity

@pytest.fixture
def config_attribute():
    return '__audited__'

@pytest.fixture
def user_class(base):
    class User(base):
        __tablename__ = 'user'
        __audited__ = {}
        id = sa.Column(sa.Integer, primary_key=True)
        name = sa.Column(sa.String(100))
        age = sa.Column(sa.Integer)
    return User


@pytest.fixture
def article_class(base):
    class Article(base):
        __tablename__ = 'article'
        __audited__ = {}
        id = sa.Column(sa.Integer, primary_key=True)
        name = sa.Column(sa.String(100))
    return Article


@pytest.yield_fixture
def versioning_manager(base, config_attribute):
    vm = VersioningManager(config_attribute=config_attribute)
    vm.init(base)
    yield vm
    vm.remove_listeners()

@pytest.mark.usefixtures('versioning_manager', 'table_creator')
class TestCustomConfigAttribute(object):
    def test_insert(self, user, connection, schema_name):
        activity = last_activity(connection, schema=schema_name)
        assert activity['old_data'] == {}
        assert activity['changed_data'] == {
            'id': user.id,
            'name': 'John',
            'age': 15
        }
        assert activity['table_name'] == 'user'
        assert activity['native_transaction_id'] > 0
        assert activity['verb'] == 'insert'
