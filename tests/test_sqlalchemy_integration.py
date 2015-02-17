# -*- coding: utf-8 -*-
import pytest
from flexmock import flexmock

from postgresql_audit import activity_values, versioning_manager
from .utils import last_activity


@pytest.fixture(scope='module')
def activity_cls(base):
    versioning_manager.init(base)
    return versioning_manager.activity_cls


@pytest.mark.usefixtures('activity_cls', 'table_creator')
class TestActivityCreation(object):
    def test_insert(self, user, connection):
        activity = last_activity(connection)
        assert activity['object_id'] == str(user.id)
        assert activity['changed_fields'] is None
        assert activity['row_data'] == {
            'id': str(user.id),
            'name': 'John',
            'age': '15'
        }
        assert activity['table_name'] == 'user'
        assert activity['transaction_id'] > 0
        assert activity['verb'] == 'insert'

    def test_activity_values_context_manager(
        self,
        activity_cls,
        user_class,
        session
    ):
        with activity_values(session.connection(), target_id=1):
            user = user_class(name='John')
            session.add(user)
            session.commit()

        activity = last_activity(session)
        assert activity['target_id'] == '1'

    def test_operation_after_commit(
        self,
        activity_cls,
        user_class,
        session
    ):
        with activity_values(session.connection(), target_id=1):
            user = user_class(name='Jack')
            session.add(user)
            session.commit()
        with activity_values(session.connection(), target_id=1):
            user = user_class(name='Jack')
            session.add(user)
            session.commit()
        activity = last_activity(session)
        assert session.query(activity_cls).count() == 2
        assert activity['target_id'] == '1'

    def test_operation_after_rollback(
        self,
        activity_cls,
        user_class,
        session
    ):
        assert session.query(activity_cls).count() == 0
        with activity_values(session.connection(), target_id=1):
            user = user_class(name='John')
            session.add(user)
            session.rollback()
        with activity_values(session.connection(), target_id=1):
            user = user_class(name='John')
            session.add(user)
            session.commit()
        activity = last_activity(session)
        assert session.query(activity_cls).count() == 1
        assert activity['target_id'] == '1'

    def test_activity_values_scope(
        self,
        activity_cls,
        user_class,
        session
    ):
        with activity_values(session.connection(), target_id=1):
            user = user_class(name='John')
            session.add(user)
            session.commit()
        with activity_values(session.connection(), actor_id=1):
            user = user_class(name='John')
            session.add(user)
            session.commit()
        activity = last_activity(session)
        assert session.query(activity_cls).count() == 2
        assert activity['actor_id'] == '1'
        assert activity['target_id'] is None

    def test_manager_defaults(
        self,
        user_class,
        session
    ):
        versioning_manager.values = {'actor_id': 1}
        user = user_class(name='John')
        session.add(user)
        session.commit()
        activity = last_activity(session)
        assert activity['actor_id'] == '1'

    def test_raw_insert(
        self,
        user_class,
        session
    ):
        versioning_manager.values = {'actor_id': 1}
        session.execute(user_class.__table__.insert().values(name='John'))
        activity = last_activity(session)
        assert activity['actor_id'] == '1'

    def test_keeps_track_of_created_tables(
        self,
        user_class,
        session
    ):
        # The activity_values table should already be created in test setup
        # phase
        (
            flexmock(versioning_manager)
            .should_receive('create_temp_table')
            .never()
        )
        versioning_manager.values = {'actor_id': 1}
        session.execute(user_class.__table__.insert().values(name='John'))
        session.execute(user_class.__table__.insert().values(name='John'))
        activity = last_activity(session)

        assert activity['actor_id'] == '1'

    def test_connection_cleaning(self, user_class, connection):
        assert len(versioning_manager.connections_with_tables) == 1
        assert len(versioning_manager.connections_with_tables_row) == 1

    def test_activity_repr(self, activity_cls):
        assert repr(activity_cls(id=3, table_name='user')) == (
            "<Activity table_name='user' id=3>"
        )