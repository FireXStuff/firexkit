
import unittest
from celery import Celery

from firexkit.argument_conversion import ConverterRegister
from firexkit.bag_of_goodies import BagOfGoodies
from firexkit.chain import returns
from firexkit.task import FireXTask, task_prerequisite


class TaskTests(unittest.TestCase):

    def test_instantiation(self):
        from celery.utils.threads import LocalStack

        with self.subTest("Without overrides"):
            # Make sure you can instantiate without the need for the pre and post overrides
            # noinspection PyAbstractClass
            class TestTask(FireXTask):
                name = self.__module__ + "." + self.__class__.__name__ + "." + "TestClass"

                def run(self):
                    pass

            test_obj = TestTask()
            self.assertIsNotNone(test_obj, "Task object not instantiated")
            self.assertTrue(callable(test_obj.undecorated))

            test_obj.request_stack = LocalStack()  # simulate binding
            test_obj()

        with self.subTest("With overrides"):
            # create a class using the override
            class TestTask(FireXTask):
                ran = False
                pre_ran = False
                post_ran = False
                name = self.__module__ + "." + self.__class__.__name__ + "." + "TestClass"

                def pre_task_run(self, bag_of_goodies: BagOfGoodies):
                    TestTask.pre_ran = True

                def run(self):
                    TestTask.ran = True

                def post_task_run(self, results, bag_of_goodies: BagOfGoodies):
                    TestTask.post_ran = True

            test_obj = TestTask()
            self.assertIsNotNone(test_obj, "Task object not instantiated")
            self.assertTrue(callable(test_obj.undecorated))

            test_obj.request_stack = LocalStack()  # simulate binding
            test_obj()
            self.assertTrue(TestTask.pre_ran, "pre_task_run() was not called")
            self.assertTrue(TestTask.ran, "run() was not called")
            self.assertTrue(TestTask.post_ran, "post_task_run() was not called")

        with self.subTest("Must have Run"):
            # noinspection PyAbstractClass
            class TestTask(FireXTask):
                name = self.__module__ + "." + self.__class__.__name__ + "." + "TestClass"
            test_obj = TestTask()
            test_obj.request_stack = LocalStack()  # simulate binding
            with self.assertRaises(NotImplementedError):
                test_obj()

    def test_task_argument_conversion(self):
        from firexkit.argument_conversion import ConverterRegister
        from celery.utils.threads import LocalStack

        # noinspection PyAbstractClass
        class TestTask(FireXTask):
            name = self.__module__ + "." + self.__class__.__name__ + "." + "TestClass"
            pre_ran = False
            post_ran = False

            def run(self):
                pass

        @ConverterRegister.register_for_task(TestTask, True)
        def pre(_):
            TestTask.pre_ran = True

        @ConverterRegister.register_for_task(TestTask, False)
        def post(_):
            TestTask.post_ran = True

        test_obj = TestTask()
        test_obj.request_stack = LocalStack()  # simulate binding
        test_obj()
        self.assertTrue(TestTask.pre_ran, "pre_task_run() was not called")
        self.assertTrue(TestTask.post_ran, "post_task_run() was not called")

    def test_undecorated(self):
        test_app = Celery()

        # noinspection PyUnusedLocal
        @test_app.task(base=FireXTask, bind=True)
        def a(myself, something):
            return something

        @test_app.task(base=FireXTask)
        def b(something):
            return something

        # noinspection PyUnusedLocal
        @test_app.task(base=FireXTask, bind=True)
        @returns('something')
        def c(myself, something):
            return something

        @test_app.task(base=FireXTask)
        @returns('something')
        def d(something):
            return something

        for micro in [a, b, c, d]:
            with self.subTest(micro):
                the_sent_something = "something"
                result = micro.undecorated(the_sent_something)
                self.assertEqual(the_sent_something, result)

    def test_prerequisite(self):
        test_app = Celery()

        @test_app.task(base=FireXTask)
        def something():
            # Should not reach here
            pass  # pragma: no cover

        @task_prerequisite(something, trigger=lambda _: False)
        @test_app.task(base=FireXTask)
        def needs_a_little_something():
            # Should not reach here
            pass  # pragma: no cover

        self.assertTrue(len(ConverterRegister.list_converters(needs_a_little_something.__name__)) == 1)

        with self.assertRaises(Exception):
            @task_prerequisite(something, trigger=None)
            @test_app.task(base=FireXTask)
            def go_boom():
                # Should not reach here
                pass  # pragma: no cover
