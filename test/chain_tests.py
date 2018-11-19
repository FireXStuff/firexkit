import unittest
from celery import Celery
from collections import namedtuple
from firexkit.task import FireXTask
from firexkit.chain import ReturnsCodingException, returns
from functools import wraps


class ReturnsTests(unittest.TestCase):

    def test_returns_normal_case(self):
        test_app = Celery()

        @test_app.task(base=FireXTask)
        @returns("stuff")
        def a_task(the_goods):
            return the_goods

        ret = a_task(the_goods="the_goods")
        self.assertTrue(type(ret) is dict)
        self.assertTrue(len(ret) == 2)
        self.assertTrue("stuff" in ret)
        self.assertTrue("the_goods" in ret)
        self.assertEqual("the_goods", ret["the_goods"])
        self.assertEqual(ret["stuff"], ret["the_goods"])

    def test_bad_returns_code(self):
        test_app = Celery()

        # duplicate keys
        with self.assertRaises(ReturnsCodingException):
            @test_app.task(base=FireXTask)
            @returns("a", "a")
            def a_task():
                # Should not reach here
                pass  # pragma: no cover

        # @returns above @app.task
        with self.assertRaises(ReturnsCodingException):
            @returns("a")
            @test_app.task(base=FireXTask)
            def a_task():
                # Should not reach here
                pass  # pragma: no cover

        # no keys in @returns
        with self.assertRaises(ReturnsCodingException):
            @test_app.task(base=FireXTask)
            @returns()
            def a_task():
                # Should not reach here
                pass  # pragma: no cover

        # No actual return
        with self.assertRaises(ReturnsCodingException):
            @test_app.task(base=FireXTask)
            @returns("a", "b")
            def a_task():
                return
            a_task()

        # values returned don't match keys
        with self.assertRaises(ReturnsCodingException):
            @test_app.task(base=FireXTask)
            @returns("a", "b")
            def another_task():
                return None, None, None
            another_task()

    def test_returns_and_bind(self):
        test_app = Celery()

        @test_app.task(base=FireXTask, bind=True)
        @returns("the_task_name", "stuff")
        def a_task(task_self, the_goods):
            return task_self.name, the_goods

        ret = a_task(the_goods="the_goods")
        self.assertTrue(type(ret) is dict)
        self.assertTrue(len(ret) == 3)
        self.assertTrue("stuff" in ret)
        self.assertTrue("the_task_name" in ret)
        self.assertTrue("the_goods" in ret)
        # noinspection PyTypeChecker
        self.assertEqual("the_goods", ret["the_goods"])
        # noinspection PyTypeChecker
        self.assertEqual(ret["stuff"], ret["the_goods"])

    def test_returns_play_nice_with_decorators(self):
        test_app = Celery()

        def passing_through(func):
            @wraps(func)
            def inner(*args, **kwargs):
                return func(*args, **kwargs)
            return inner

        @test_app.task(base=FireXTask)
        @returns("stuff")
        @passing_through
        def a_task(the_goods):
            return the_goods

        ret = a_task(the_goods="the_goods")
        self.assertTrue(type(ret) is dict)
        self.assertTrue(len(ret) == 2)
        self.assertTrue("stuff" in ret)

    def test_returning_named_tuples(self):
        test_app = Celery()
        TestingTuple = namedtuple('TestingTuple', ['thing1', 'thing2'])

        @test_app.task(base=FireXTask)
        @returns("named_t")
        def a_task():
            return TestingTuple(thing1=1, thing2="two")
        ret = a_task()

        # noinspection PyTypeChecker
        self.assertTrue(type(ret["named_t"]) is TestingTuple)
        self.assertTrue(type(ret) is dict)
        self.assertTrue(len(ret) == 1)
        self.assertTrue("named_t" in ret)

        # noinspection PyTypeChecker
        self.assertEqual(1, ret["named_t"].thing1)
        # noinspection PyTypeChecker
        self.assertEqual("two", ret["named_t"].thing2)
