import inspect
import unittest

from firexkit.bag_of_goodies import BagOfGoodies


class BagTests(unittest.TestCase):
    def test_first_unbound_microservice(self):
        # noinspection PyUnusedLocal
        def func(one, two, three): pass
        sig = inspect.signature(func)

        args = tuple()  # first task of chain gets empty tuple
        kwargs = {'loglevel': 'debug',
                  'chain': 'noop',
                  'submission_dir': '/ws/mdelahou-ott/firex7',
                  'plugins': '',
                  'argv': ['/ws/mdelahou-ott/firex7/firex.py', 'submit', '--chain', 'noop', '--no_email', '--sync'],
                  'unconverted_chain_args': {'disable_blaze': True},
                  'concurrent_runs_limit': 6,
                  'copy_on_task_failure': False,
                  'sync': True,
                  'no_email': True,
                  'one': 'mdelahou'}
        bog = BagOfGoodies(sig, args, kwargs)

        goodies = bog.get_bag()
        self.assertDictEqual(goodies, kwargs, "bag of goodies should be same as kwargs")
        new_data = {"random": 0}
        kwargs.update(new_data)
        self.assertNotEqual(len(kwargs), len(bog.get_bag()), "Updating kwargs should not affect the bog")
        bog.update(new_data)
        self.assertDictEqual(goodies, kwargs, "bag of goodies should be same as kwargs after update")
        mod_value = {"chain": "something"}
        kwargs.update(mod_value)
        bog.update(mod_value)
        self.assertDictEqual(goodies, kwargs, "bag of goodies should be same as kwargs after update to existing key")

    def test_first_with_explicit_args(self):

        # noinspection PyUnusedLocal
        def a_simple_task(value, value2="asdf"): pass
        sig = inspect.signature(a_simple_task)
        args = ('nope',)
        kwargs = {}
        bog = BagOfGoodies(sig, args, kwargs)
        self.assertDictEqual(bog.get_bag(),
                             {"value": 'nope'})
        bog.update({"value": 'yup'})
        self.assertDictEqual(bog.get_bag(),
                             {"value": 'yup'})
        a, k = bog.split_for_signature()
        self.assertEqual(len(a), 1)
        self.assertEqual(a[0], 'yup')

    def test_chained_unbound_microservice(self):
        # noinspection PyUnusedLocal
        def func(one, two, three=0): pass
        sig = inspect.signature(func)

        old_bog = {'loglevel': 'debug',
                   'chain': 'noop',
                   'submission_dir': '/ws/mdelahou-ott/firex7',
                   'plugins': '',
                   'argv': ['/ws/mdelahou-ott/firex7/firex.py', 'submit', '--chain', 'noop', '--no_email', '--sync'],
                   'unconverted_chain_args': {'disable_blaze': True},
                   'concurrent_runs_limit': 6,
                   'copy_on_task_failure': False,
                   'sync': True,
                   'no_email': True,
                   'one': 'mdelahou'}
        args = (old_bog,)
        kwargs = {}
        bog = BagOfGoodies(sig, args, kwargs)

        goodies = bog.get_bag()
        self.assertDictEqual(goodies, old_bog, "bag of goodies should be same as kwargs")
        new_data = {"random": 0}
        old_bog.update(new_data)
        self.assertNotEqual(len(old_bog), len(bog.get_bag()), "Updating old bog should not affect the bog")
        bog.update(new_data)
        self.assertDictEqual(goodies, old_bog, "bag of goodies should be same as old_bog after update")
        mod_value = {"chain": "something"}
        old_bog.update(mod_value)
        bog.update(mod_value)
        self.assertDictEqual(goodies, old_bog, "bag of goodies should be same as old_bog after update to existing key")

    def test_indirect_from_return(self):

        # noinspection PyUnusedLocal
        def ending(final, missing):
            self.assertTrue(True)
        sig = inspect.signature(ending)

        old_bog = {
                    'concurrent_runs_limit': 6,
                    'cc_firex': False, 'loglevel': 'debug', 'sync': True, 'start': 'yep', 'group': 'mdelahou',
                    'plugins': '/ws/mdelahou-ott/firex7/flow_tests/prio1/argument_validation_tests.py',
                    'no_email': True,
                    'final': 'pass',   # <-- This is what we are testing
                    'submission_dir': '/ws/mdelahou-ott/firex7',
                    'unconverted_chain_args': {'missing': '@final', 'cc_firex': 'False', 'start': 'yep',
                                               'disable_blaze': True,
                                               'original_program': '/ws/mdelahou-ott/firex7/firex_bin/firex',
                                               'plugins': '/ws/mdelahou-ott/firex7/flow_tests/prio1/'
                                                          'argument_validation_tests.py'},
                    'copy_on_task_failure': False, 'chain': 'beginning,ending',
                    'missing': '@final',  # <-- This is what we are testing
                    'original_program': '/ws/mdelahou-ott/firex7/firex_bin/firex',
                    'argv': ['/ws/mdelahou-ott/firex7/firex.py', 'submit', '--chain', 'beginning,ending', '--start',
                             'yep', '--missing', '@final', '--sync', '--no_email', '--cc_firex', 'False',
                             '--monitor_verbosity', 'none', '--flame_port', '54560', '--flame_timeout', '60',
                             '--disable_blaze', 'True', '--external',
                             '/ws/mdelahou-ott/firex7/flow_tests/prio1/argument_validation_tests.py']}
        args = (old_bog,)
        kwargs = {}
        bog = BagOfGoodies(sig, args, kwargs)

        self.assertTrue('missing' in bog.get_bag())
        self.assertEqual(bog.get_bag()['missing'], 'pass')

        a, k = bog.split_for_signature()
        ending(*a, **k)
        self.assertTrue(len(k), 2)
        self.assertEqual(k['missing'], 'pass')

        # noinspection PyUnusedLocal
        def ending_v2(final, missing, **the_kwargs):
            self.assertTrue(True)
        sig_v2 = inspect.signature(ending_v2)
        bog_v2 = BagOfGoodies(sig_v2, args, kwargs)
        a, k = bog_v2.split_for_signature()
        ending_v2(*a, **k)
        self.assertTrue(len(k), 16)

    def test_indirect_from_default(self):
        def fun(arg_zero,
                arg_one="@dont_use",
                arg_two="@first_value",
                arg_three="@second_value",
                arg_four="@dont_use"):
            self.assertEqual('success', arg_zero)
            self.assertEqual('success', arg_one)
            self.assertEqual('success', arg_two)
            self.assertEqual('success', arg_three)
            self.assertEqual('success', arg_four)
        sig = inspect.signature(fun)

        old_bog = {'first_value': 'success'}
        args = (old_bog, "@first_value", "success")
        kwargs = {"second_value": 'success',
                  "dont_use": "failure",
                  "arg_four": "success"}
        bog = BagOfGoodies(sig, args, kwargs)
        a, k = bog.split_for_signature()
        fun(*a, **k)

    def test_existing_goodies_and_extra(self):
        old_bog = {'geo_loc': 'OTT'}
        args = (old_bog,)
        kwargs = {'expected_module': 'additional_for_plugin_propagation.py'}

        # noinspection PyUnusedLocal
        def from_another_test(expected_module):
            pass
        sig = inspect.signature(from_another_test)
        bog = BagOfGoodies(sig, args, kwargs)

        combined = {}
        combined.update(old_bog)
        combined.update(kwargs)
        self.assertDictEqual(bog.get_bag(), combined)

    def test_bind_goodies(self):
        old_bog = {'geo_loc': 'OTT'}
        args = (old_bog,)
        kwargs = {'expected_module': 'additional_for_plugin_propagation.py'}
        a_self = self

        class Thing:
            # noinspection PyMethodMayBeStatic
            def a_func(self, expected_module):
                a_self.assertEqual(expected_module, "additional_for_plugin_propagation.py")
        t = Thing()
        sig = inspect.signature(t.a_func)
        bog = BagOfGoodies(sig, args, kwargs)
        a, k = bog.split_for_signature()
        t.a_func(*a, **k)
