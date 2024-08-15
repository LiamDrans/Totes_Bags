from pytest import mark
from db.crud_functions import query_db

@mark.describe('testing query_db')
class TestQueryDB:

    @mark.it('Test connection to database')
    def test_query_db(self):
        query_db
        assert False