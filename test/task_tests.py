
import unittest
from celery import Celery

from firexkit.argument_conversion import ConverterRegister
from firexkit.chain import returns
from firexkit.task import FireXTask, task_prerequisite, get_all_args_of_task


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

                def pre_task_run(self):
                    TestTask.pre_ran = True

                def run(self):
                    TestTask.ran = True

                def post_task_run(self, results):
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

    def test_properties(self):
        test_app = Celery()

        # noinspection PyUnusedLocal
        @test_app.task(base=FireXTask, bind=True)
        def a(myself, arg1):
            pass

        # noinspection PyUnusedLocal
        @test_app.task(base=FireXTask, bind=True)
        def b(myself, arg1=None):
            pass

        # noinspection PyUnusedLocal
        @test_app.task(base=FireXTask, bind=True)
        def c(myself, arg1, arg2=None):
            pass

        with self.subTest('One required argument'):
            value = 1
            a(value)
            self.assertListEqual(a.args, [value])
            self.assertDictEqual(a.kwargs, {})
            self.assertListEqual(a.required_args, ['arg1'])
            self.assertDictEqual(a.bound_args, {'arg1': value})
            self.assertDictEqual(a.default_bound_args, {})
            self.assertDictEqual(a.all_args, {'arg1': value})
            self.assertDictEqual(a.bag, {'arg1': value})
            self.assertDictEqual(a.abog, {'arg1': value})

        with self.subTest('One required argument with keyword'):
            value = 1
            a(arg1=value)
            self.assertListEqual(a.args, [])
            self.assertDictEqual(a.kwargs, {'arg1': value})
            self.assertListEqual(a.required_args, ['arg1'])
            self.assertDictEqual(a.bound_args, {'arg1': value})
            self.assertDictEqual(a.default_bound_args, {})
            self.assertDictEqual(a.all_args, {'arg1': value})
            self.assertDictEqual(a.bag, {'arg1': value})
            self.assertDictEqual(a.abog, {'arg1': value})

        with self.subTest('One optional argument'):
            value = 1
            b(value)
            self.assertListEqual(b.args, [value])
            self.assertDictEqual(b.kwargs, {})
            self.assertListEqual(b.required_args, [])
            self.assertDictEqual(b.bound_args, {'arg1': value})
            self.assertDictEqual(b.default_bound_args, {})
            self.assertDictEqual(b.all_args, {'arg1': value})
            self.assertDictEqual(b.bag, {'arg1': value})
            self.assertDictEqual(b.abog, {'arg1': value})

        with self.subTest('One optional argument with no value'):
            value=None
            b()
            self.assertListEqual(b.args, [])
            self.assertDictEqual(b.kwargs, {})
            self.assertListEqual(b.required_args, [])
            self.assertDictEqual(b.bound_args, {})
            self.assertDictEqual(b.default_bound_args, {'arg1': value})
            self.assertDictEqual(b.all_args, {'arg1': value})
            self.assertDictEqual(b.bag, {})
            self.assertDictEqual(b.abog, {'arg1': value})

        with self.subTest('One optional argument with keyword'):
            value = 1
            b(arg1=value)
            self.assertListEqual(b.args, [])
            self.assertDictEqual(b.kwargs, {'arg1': value})
            self.assertListEqual(b.required_args, [])
            self.assertDictEqual(b.bound_args, {'arg1': value})
            self.assertDictEqual(b.default_bound_args, {})
            self.assertDictEqual(b.all_args, {'arg1': value})
            self.assertDictEqual(b.bag, {'arg1': value})
            self.assertDictEqual(b.abog, {'arg1': value})

        with self.subTest('One required and one optional argument '):
            value1 = 1
            value2 = 2
            c(value1, value2)
            self.assertListEqual(c.args, [value1, value2])
            self.assertDictEqual(c.kwargs, {})
            self.assertListEqual(c.required_args, ['arg1'])
            self.assertDictEqual(c.bound_args, {'arg1': value1,
                                                'arg2': value2})
            self.assertDictEqual(c.default_bound_args, {})
            self.assertDictEqual(c.all_args, {'arg1': value1,
                                              'arg2': value2})
            self.assertDictEqual(c.bag, {'arg1': value1,
                                         'arg2': value2})
            self.assertDictEqual(c.abog, {'arg1': value1,
                                          'arg2': value2})

        with self.subTest('One required and one optional argument with keyword'):
            value1 = 1
            value2 = 2
            c(arg2=value2, arg1=value1)
            self.assertListEqual(c.args, [])
            self.assertDictEqual(c.kwargs, {'arg1': value1,
                                            'arg2': value2})
            self.assertListEqual(c.required_args, ['arg1'])
            self.assertDictEqual(c.bound_args, {'arg1': value1,
                                                'arg2': value2})
            self.assertDictEqual(c.default_bound_args, {})
            self.assertDictEqual(c.all_args, {'arg1': value1,
                                              'arg2': value2})
            self.assertDictEqual(c.bag, {'arg1': value1,
                                         'arg2': value2})
            self.assertDictEqual(c.abog, {'arg1': value1,
                                          'arg2': value2})

        with self.subTest('One required, one optional provided'):
            value1 = 1
            value2 = None
            c(value1)
            self.assertListEqual(c.args, [value1])
            self.assertDictEqual(c.kwargs, {})
            self.assertListEqual(c.required_args, ['arg1'])
            self.assertDictEqual(c.bound_args, {'arg1': value1})
            self.assertDictEqual(c.default_bound_args, {'arg2': value2})
            self.assertDictEqual(c.all_args, {'arg1': value1,
                                              'arg2': value2})
            self.assertDictEqual(c.bag, {'arg1': value1})
            self.assertDictEqual(c.abog, {'arg1': value1,
                                          'arg2': value2})

    def test_sig_bind(self):
        test_app = Celery()

        # noinspection PyUnusedLocal
        @test_app.task(base=FireXTask, bind=True)
        def a(myself, arg1):
            pass

        # noinspection PyUnusedLocal
        @test_app.task(base=FireXTask, bind=True)
        def b(myself, arg1=None):
            pass

        # noinspection PyUnusedLocal
        @test_app.task(base=FireXTask, bind=True)
        def c(myself, arg1, arg2=None):
            pass

        with self.subTest():
            value = 1
            r = get_all_args_of_task(a, value)
            expected_result = {'arg1': value}
            self.assertDictEqual(r, expected_result)

        with self.subTest():
            with self.assertRaises(TypeError):
                get_all_args_of_task(a)

        with self.subTest():
            input_args = {'arg1': 1}
            r = get_all_args_of_task(a, **input_args)
            self.assertDictEqual(r, input_args)

        with self.subTest():
            expected_result = {'arg1': 1}
            other = {'arg2': 2}
            input_args = {**expected_result, **other}
            r = get_all_args_of_task(a, **input_args)
            self.assertDictEqual(r, expected_result)

        with self.subTest():
            expected_result = {'arg1': None}
            r = get_all_args_of_task(b)
            self.assertDictEqual(r, expected_result)

        with self.subTest():
            input_relevant = {'arg1': 1}
            other = {'arg3': 3}
            input_args = {**input_relevant, **other}
            r = get_all_args_of_task(c, **input_args)
            default = {'arg2': None}
            expected_result = {**input_relevant, **default}
            self.assertDictEqual(r, expected_result)

        with self.subTest():
            input_args = {'arg1': 1, 'arg2': 2}
            r = get_all_args_of_task(c, **input_args)
            self.assertDictEqual(r, input_args)

        with self.subTest():
            value = 1
            input_args = {'arg2': 2}
            r = get_all_args_of_task(c, value, **input_args)
            expected_result = {'arg1': value, **input_args}
            self.assertDictEqual(r, expected_result)

        with self.subTest():
            input_args = {'arg2': 2}
            with self.assertRaises(TypeError):
                get_all_args_of_task(c, **input_args)
