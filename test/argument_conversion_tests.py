
import unittest
from firexkit.argument_convertion import ConverterRegister, CircularDependencyException, MissingConverterDependencyError


class ArgConversionTests(unittest.TestCase):

    def test_converter_registration(self):
        test_input_converter = ConverterRegister()

        @test_input_converter.register
        def converter_no_dependency(kwargs):
            kwargs['converter_no_dependency'] = True
            return kwargs

        @test_input_converter.register('converter_no_dependency')
        def converter_str_dependency(kwargs):
            kwargs['converter_str_dependency'] = True
            return kwargs

        @test_input_converter.register('converter_no_dependency',
                                       'converter_str_dependency')
        def converter_list_dependency(kwargs):
            kwargs['converter_list_dependency'] = True
            return kwargs

        converted = test_input_converter.convert(**{})
        self.assertTrue('converter_no_dependency' in converted)
        self.assertTrue('converter_str_dependency' in converted)
        self.assertTrue('converter_list_dependency' in converted)

        test_input_converter.pre_load_was_run = False
        with self.assertRaises(MissingConverterDependencyError):
            @test_input_converter.register('Nope')
            def missing_dependent(_): pass  # Should not reach here
            test_input_converter.convert(**{})

    def test_converter_dependency(self):
        unit_test_obj = self

        test_input_converter = ConverterRegister()

        @test_input_converter.register
        def converter_one(kwargs):
            kwargs['converter_one'] = True
            return kwargs

        @test_input_converter.register('converter_one')
        def converter_two(kwargs):
            unit_test_obj.assertTrue('converter_one' in kwargs)
            kwargs['converter_two'] = True
            return kwargs

        @test_input_converter.register('converter_four')
        def converter_three(kwargs):
            unit_test_obj.assertTrue('converter_four' in kwargs)
            kwargs['converter_three'] = True
            return kwargs

        @test_input_converter.register
        def converter_four(kwargs):
            kwargs['converter_four'] = True
            return kwargs

        ############################
        # test multiple dependencies
        @test_input_converter.register('converter_one',
                                       'converter_two',
                                       'converter_three',
                                       'converter_four')
        def converter_five(kwargs):
            unit_test_obj.assertTrue('converter_one' in kwargs)
            unit_test_obj.assertTrue('converter_two' in kwargs)
            unit_test_obj.assertTrue('converter_three' in kwargs)
            unit_test_obj.assertTrue('converter_four' in kwargs)
            return kwargs

        test_input_converter.convert(**{})

        #######################################
        # test detection of circular dependency
        test_input_converter = ConverterRegister()
        with self.assertRaises(CircularDependencyException):
            @test_input_converter.register('converter_seven')
            def converter_six(_): pass  # Should not reach here

            @test_input_converter.register('converter_eight')
            def converter_seven(_): pass  # Should not reach here

            @test_input_converter.register('converter_six')
            def converter_eight(_): pass  # Should not reach here
            test_input_converter.convert(**{})

        ################################
        # test unrecognized dependencies
        test_input_converter = ConverterRegister()
        with self.assertRaises(MissingConverterDependencyError):
            @test_input_converter.register("this_is_not_valid")
            def converter_unrecognised(_): pass  # Should not reach here
            test_input_converter.convert(**{})

        #####################################################
        # test boolean to indicate after microservices loaded
        test_input_converter = ConverterRegister()

        @test_input_converter.register(True)
        def converter_nine(kwargs):
            kwargs['converter_nine'] = True

        @test_input_converter.register(False)
        def converter_ten(kwargs):
            kwargs['converter_ten'] = True

        @test_input_converter.register(False, "converter_ten")
        def converter_eleven(kwargs):
            kwargs['converter_eleven'] = True
            unit_test_obj.assertTrue('converter_ten' in kwargs)

        @test_input_converter.register("converter_eleven", False, "converter_ten")
        def converter_twelve(kwargs):
            unit_test_obj.assertTrue('converter_ten' in kwargs)
            unit_test_obj.assertTrue('converter_eleven' in kwargs)

        test_input_converter.convert(**{})
        test_input_converter.convert(pre_task=False, **{})

        #####################################################
        # test pre cannot be dependant on post
        test_input_converter = ConverterRegister()

        @test_input_converter.register(False)
        def converter_thirteen(kwargs):
            kwargs['converter_thirteen'] = True

        # post can be dependant on pre
        @test_input_converter.register(True, "converter_thirteen")
        def converter_fourteen(kwargs):
            unit_test_obj.assertTrue('converter_thirteen' in kwargs)
        kw = test_input_converter.convert(pre_task=True, **{})
        test_input_converter.convert(pre_task=False, **kw)

        @test_input_converter.register(False, "converter_fourteen")
        def converter_fifteen(_): pass  # Should not reach here

        with self.assertRaises(MissingConverterDependencyError):
            test_input_converter.convert(pre_task=True, **{})

    def test_exclude_indirect_args(self):
        test_input_converter = ConverterRegister()

        @test_input_converter.register(False)
        def no_indirect(kwargs):
            # indirect args should not be passed to converters
            self.assertTrue("excluded" not in kwargs)
            self.assertTrue("ignored" in kwargs)
            self.assertTrue(kwargs["included"])

        kw = test_input_converter.convert(pre_task=True,
                                          **{
                                              "excluded": "@included",
                                              "included": True,
                                              "ignored": "anything",
                                          })
        self.assertTrue("excluded" in kw)
        self.assertTrue("included" in kw)
        self.assertTrue("ignored" in kw)
