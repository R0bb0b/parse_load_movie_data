import sys, os, unittest

sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), "../../../../src/lib"))

from AWS.Redshift import Redshift

class S3TestCase(unittest.TestCase):
    

    def test_loadTables(self):
        class Cursor:
            def execute(self, *args,**kwargs):
                return True

            def close(self, *args,**kwargs):
                return True

        class Conn:
            def cursor(self, *args,**kwargs):
                return Cursor()

            def commit(self, *args,**kwargs):
                return True

        objRedshift = Redshift(Conn(), "foo", "bar")
        blnLoad = objRedshift.loadTables(
            {
                "foo":[{"table": "foobar", "file": "barfoo.csv"}]
            },
            "foo",
            "bar"
        )

        assert blnLoad == True

    def test_loadTables_fail_stage_creation(self):
        class Cursor:
            def execute(self, strQuery):
                if strQuery.find("staging_") != -1:
                    raise Exception("foobar")
                else:
                    return True

            def close(self, *args,**kwargs):
                return True

        class Conn:
            def cursor(self, *args,**kwargs):
                return Cursor()

            def commit(self, *args,**kwargs):
                return True

        with self.assertRaises(ValueError):
            objRedshift = Redshift(Conn(), "foo", "bar")
            blnLoad = objRedshift.loadTables(
                {
                    "foo":[{"table": "foobar", "file": "barfoo.csv"}]
                },
                "foo",
                "bar"
            )

    def test_loadTables_fail_stage_load(self):
        class Cursor:
            def execute(self, strQuery):
                if strQuery.find("IGNOREHEADER") != -1:
                    raise Exception("foobar")
                else:
                    return True

            def close(self, *args,**kwargs):
                return True

        class Conn:
            def cursor(self, *args,**kwargs):
                return Cursor()

            def commit(self, *args,**kwargs):
                return True

        with self.assertRaises(ValueError):
            objRedshift = Redshift(Conn(), "foo", "bar")
            blnLoad = objRedshift.loadTables(
                {
                    "foo":[{"table": "foobar", "file": "barfoo.csv"}]
                },
                "foo",
                "bar"
            )

    def test_loadTables_fail_swap(self):
        class Cursor:
            def execute(self, strQuery):
                if strQuery.find("alter table staging_") != -1:
                    raise Exception("foobar")
                else:
                    return True

            def close(self, *args,**kwargs):
                return True

        class Conn:
            def cursor(self, *args,**kwargs):
                return Cursor()

            def commit(self, *args,**kwargs):
                return True

        with self.assertRaises(ValueError):
            objRedshift = Redshift(Conn(), "foo", "bar")
            blnLoad = objRedshift.loadTables(
                {
                    "foo":[{"table": "foobar", "file": "barfoo.csv"}]
                },
                "foo",
                "bar"
            )
